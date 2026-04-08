import { serve } from 'https://deno.land/std@0.168.0/http/server.ts';

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

serve(async (req) => {
  if (req.method === 'OPTIONS') {
    return new Response('ok', { headers: CORS });
  }

  try {
    const { jobTitle, institution, jobSummary, cvText } = await req.json();

    const apiKey = Deno.env.get('ANTHROPIC_API_KEY');
    if (!apiKey) throw new Error('ANTHROPIC_API_KEY not configured');

    const prompt = `You are a career coach helping a job applicant prepare STAR interview talking points.

Job: ${jobTitle} at ${institution}
Job Summary: ${jobSummary?.substring(0, 1200) ?? '(not provided)'}

Applicant CV (excerpt): ${cvText?.substring(0, 2500) ?? '(not provided)'}

Generate exactly 3 STAR talking points tailored to this specific role and the applicant's background. Each point should connect a real experience from the CV to a requirement of the job.

Respond ONLY with valid JSON in this exact format (no markdown, no extra text):
{
  "points": [
    {
      "situation": "Brief context — where/when this happened",
      "task": "What you were responsible for",
      "action": "Specific steps you took",
      "result": "Measurable outcome or impact"
    }
  ]
}`;

    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': apiKey,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify({
        model: 'claude-haiku-4-5-20251001',
        max_tokens: 1200,
        messages: [{ role: 'user', content: prompt }],
      }),
    });

    if (!response.ok) {
      const err = await response.text();
      throw new Error(`Anthropic API error ${response.status}: ${err}`);
    }

    const anthropicData = await response.json();
    const raw = anthropicData?.content?.[0]?.text ?? '';

    // Parse JSON — strip any accidental markdown fences
    const jsonStr = raw.replace(/^```json\s*/i, '').replace(/^```\s*/i, '').replace(/\s*```$/i, '').trim();
    const parsed = JSON.parse(jsonStr);

    return new Response(JSON.stringify(parsed), {
      headers: { ...CORS, 'Content-Type': 'application/json' },
    });

  } catch (err) {
    return new Response(
      JSON.stringify({ error: err instanceof Error ? err.message : String(err) }),
      { status: 500, headers: { ...CORS, 'Content-Type': 'application/json' } }
    );
  }
});
