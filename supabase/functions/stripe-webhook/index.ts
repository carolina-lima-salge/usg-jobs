import { serve } from 'https://deno.land/std@0.177.0/http/server.ts';
import Stripe from 'https://esm.sh/stripe@13.11.0?target=deno';
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';

const stripe = new Stripe(Deno.env.get('STRIPE_SECRET_KEY') ?? '', {
  apiVersion: '2023-10-16',
  httpClient: Stripe.createFetchHttpClient(),
});

const supabase = createClient(
  Deno.env.get('SUPABASE_URL') ?? '',
  Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') ?? '',
);

serve(async (req) => {
  const body      = await req.text();
  const signature = req.headers.get('stripe-signature') ?? '';
  const webhookSecret = Deno.env.get('STRIPE_WEBHOOK_SECRET') ?? '';

  let event: Stripe.Event;
  try {
    event = stripe.webhooks.constructEvent(body, signature, webhookSecret);
  } catch (err) {
    return new Response(`Webhook signature failed: ${err.message}`, { status: 400 });
  }

  // Helper: update a user's subscription status by Stripe customer ID
  async function updateProfile(customerId: string, status: string, subEnd?: number | null) {
    // Find the Supabase user via customer email
    const customer = await stripe.customers.retrieve(customerId) as Stripe.Customer;
    const userId   = (customer.metadata as Record<string, string>)?.supabase_user_id;
    if (!userId) return;

    await supabase.from('profiles').update({
      subscription_status: status,
      stripe_customer_id:  customerId,
      subscription_end:    subEnd ? new Date(subEnd * 1000).toISOString() : null,
      updated_at:          new Date().toISOString(),
    }).eq('id', userId);
  }

  switch (event.type) {
    case 'checkout.session.completed': {
      const session = event.data.object as Stripe.Checkout.Session;
      if (session.mode === 'subscription' && session.customer) {
        await updateProfile(session.customer as string, 'active', null);
      }
      break;
    }
    case 'customer.subscription.updated': {
      const sub = event.data.object as Stripe.Subscription;
      const status = sub.status === 'active' ? 'active' : 'cancelled';
      await updateProfile(sub.customer as string, status, sub.current_period_end);
      break;
    }
    case 'customer.subscription.deleted': {
      const sub = event.data.object as Stripe.Subscription;
      await updateProfile(sub.customer as string, 'cancelled', null);
      break;
    }
    case 'invoice.payment_failed': {
      const invoice = event.data.object as Stripe.Invoice;
      if (invoice.customer) {
        await updateProfile(invoice.customer as string, 'past_due', null);
      }
      break;
    }
  }

  return new Response(JSON.stringify({ received: true }), {
    headers: { 'Content-Type': 'application/json' },
    status: 200,
  });
});
