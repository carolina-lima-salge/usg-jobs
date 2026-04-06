/**
 * Cloudflare Worker — Stripe Webhook Handler for USG Jobs
 * =========================================================
 * Listens for Stripe checkout events and updates the Supabase
 * profiles table so subscribers get access immediately after paying.
 *
 * Required Cloudflare Worker secrets (set via dashboard or wrangler):
 *   STRIPE_WEBHOOK_SECRET   — from Stripe Dashboard → Webhooks → Signing secret
 *   SUPABASE_URL            — e.g. https://xxxx.supabase.co
 *   SUPABASE_SERVICE_ROLE_KEY — from Supabase → Settings → API (service_role key)
 *
 * Stripe events to enable on this webhook endpoint:
 *   checkout.session.completed
 *   customer.subscription.deleted
 *
 * Deploy steps:
 *   1. Go to Cloudflare Workers & Pages → Create Worker
 *   2. Paste this file as the worker code
 *   3. Add the three secrets above under Worker → Settings → Variables
 *   4. Copy the Worker URL (e.g. https://usg-jobs-stripe.yourname.workers.dev)
 *   5. In Stripe Dashboard → Developers → Webhooks → Add endpoint
 *      - Endpoint URL: <your worker URL>
 *      - Events: checkout.session.completed, customer.subscription.deleted
 *   6. Copy the "Signing secret" from Stripe and save as STRIPE_WEBHOOK_SECRET
 */

export default {
  async fetch(request, env) {
    if (request.method !== 'POST') {
      return new Response('Method not allowed', { status: 405 });
    }

    const sig  = request.headers.get('stripe-signature');
    const body = await request.text();

    // ── Verify Stripe signature ───────────────────────────────────────────────
    let event;
    try {
      event = await verifyStripeSignature(body, sig, env.STRIPE_WEBHOOK_SECRET);
    } catch (e) {
      console.error('Stripe signature verification failed:', e.message);
      return new Response('Webhook signature invalid: ' + e.message, { status: 400 });
    }

    // ── Handle events ─────────────────────────────────────────────────────────
    try {
      if (event.type === 'checkout.session.completed') {
        await handleCheckoutCompleted(event.data.object, env);

      } else if (event.type === 'customer.subscription.deleted') {
        await handleSubscriptionDeleted(event.data.object, env);

      }
      // All other events are acknowledged but ignored
    } catch (e) {
      console.error('Handler error:', e.message);
      return new Response('Handler error: ' + e.message, { status: 500 });
    }

    return new Response('OK', { status: 200 });
  },
};

// ── checkout.session.completed ─────────────────────────────────────────────────
// client_reference_id is set to "<supabase_user_id>|<tier>" by startStripeCheckout()
async function handleCheckoutCompleted(session, env) {
  const ref = session.client_reference_id || '';
  if (!ref) {
    console.warn('checkout.session.completed received with no client_reference_id — skipping');
    return;
  }

  const [userId, tier] = ref.split('|');
  if (!userId) {
    console.warn('Could not parse userId from client_reference_id:', ref);
    return;
  }

  const resolvedTier = tier === 'pro' ? 'pro' : 'basic';
  await updateProfile(userId, 'active', resolvedTier, env);
  console.log(`Activated ${resolvedTier} for user ${userId}`);
}

// ── customer.subscription.deleted ─────────────────────────────────────────────
// When a subscription is cancelled in Stripe, deactivate in Supabase too.
// We store the Stripe customer ID in profiles so we can look up the user.
async function handleSubscriptionDeleted(subscription, env) {
  const stripeCustomerId = subscription.customer;
  if (!stripeCustomerId) return;

  // Find the Supabase user with this Stripe customer ID
  const searchUrl = `${env.SUPABASE_URL}/rest/v1/profiles?stripe_customer_id=eq.${encodeURIComponent(stripeCustomerId)}&select=id`;
  const res = await fetch(searchUrl, {
    headers: {
      'apikey':        env.SUPABASE_SERVICE_ROLE_KEY,
      'Authorization': `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
    },
  });

  if (!res.ok) {
    console.warn('Could not look up user by stripe_customer_id:', stripeCustomerId);
    return;
  }

  const rows = await res.json();
  if (!rows.length) {
    console.warn('No profile found for stripe_customer_id:', stripeCustomerId);
    return;
  }

  await updateProfile(rows[0].id, null, null, env);
  console.log(`Deactivated subscription for user ${rows[0].id}`);
}

// ── Supabase PATCH ─────────────────────────────────────────────────────────────
async function updateProfile(userId, status, tier, env) {
  const url = `${env.SUPABASE_URL}/rest/v1/profiles?id=eq.${encodeURIComponent(userId)}`;
  const payload = {
    subscription_status: status,
    subscription_tier:   tier,
    updated_at:          new Date().toISOString(),
  };

  const res = await fetch(url, {
    method: 'PATCH',
    headers: {
      'Content-Type':  'application/json',
      'apikey':        env.SUPABASE_SERVICE_ROLE_KEY,
      'Authorization': `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
      'Prefer':        'return=minimal',
    },
    body: JSON.stringify(payload),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Supabase PATCH failed (${res.status}): ${text}`);
  }
}

// ── Stripe signature verification (Web Crypto — no npm needed) ─────────────────
async function verifyStripeSignature(payload, sigHeader, secret) {
  if (!sigHeader) throw new Error('Missing stripe-signature header');
  if (!secret)    throw new Error('STRIPE_WEBHOOK_SECRET not configured');

  // Parse "t=...,v1=...,v1=..." header
  let timestamp = null;
  const signatures = [];
  for (const part of sigHeader.split(',')) {
    const eq = part.indexOf('=');
    if (eq === -1) continue;
    const k = part.slice(0, eq).trim();
    const v = part.slice(eq + 1).trim();
    if (k === 't')  timestamp  = v;
    if (k === 'v1') signatures.push(v);
  }

  if (!timestamp || signatures.length === 0) {
    throw new Error('Malformed stripe-signature header');
  }

  // Reject if older than 5 minutes
  const now = Math.floor(Date.now() / 1000);
  if (Math.abs(now - parseInt(timestamp, 10)) > 300) {
    throw new Error('Stripe timestamp too old (possible replay attack)');
  }

  // HMAC-SHA256 of "timestamp.payload"
  const signedPayload = `${timestamp}.${payload}`;
  const key = await crypto.subtle.importKey(
    'raw',
    new TextEncoder().encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign'],
  );
  const mac = await crypto.subtle.sign(
    'HMAC',
    key,
    new TextEncoder().encode(signedPayload),
  );
  const expectedHex = Array.from(new Uint8Array(mac))
    .map(b => b.toString(16).padStart(2, '0'))
    .join('');

  if (!signatures.includes(expectedHex)) {
    throw new Error('Stripe signature mismatch');
  }

  return JSON.parse(payload);
}
