/**
 * star-prep — Supabase Edge Function
 *
 * Generates STAR (Situation, Task, Action, Result) interview talking points
 * by matching a candidate's CV against a specific job description using Claude.
 *
 * Deploy:
 *   supabase link --project-ref wktobsxgcpcjuahtmgxa
 *   supabase secrets set ANTHROPIC_API_KEY=sk-ant-...
 *   supabase functions deploy star-prep
 */

import { serve } from "https://deno.land/std@0.177.0/http/server.ts";

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

serve(async (req: Request) => {
  // Handle CORS preflight
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: CORS_HEADERS });
  }

  try {
    const { jobTitle, institution, jobSummary, cvText } = await req.json();

    if (!cvText || !jobTitle) {
      return new Response(
        JSON.stringify({ error: "jobTitle and cvText are required" }),
        { status: 400, headers: { ...CORS_HEADERS, "Content-Type": "application/json" } }
      );
    }

    const apiKey = Deno.env.get("ANTHROPIC_API_KEY");
    if (!apiKey) {
      return new Response(
        JSON.stringify({ error: "ANTHROPIC_API_KEY not configured" }),
        { status: 500, headers: { ...CORS_HEADERS, "Content-Type": "application/json" } }
      );
    }

    const prompt = `You are an expert career coach helping a candidate prepare for a job interview.

Job: ${jobTitle}${institution ? ` at ${institution}` : ""}
Job Description:
${jobSummary || "(no description provided)"}

Candidate CV (excerpt):
${cvText.substring(0, 4000)}

Your task: Generate exactly 3 specific, interview-ready STAR talking points this candidate could use for this role.

Requirements for each point:
- Draw on REAL experience from the candidate's CV
- Make it specific to THIS job's requirements
- Each should be something the candidate could actually say in an interview
- Be concrete — include skills, tools, or outcomes from their background

Return ONLY a JSON object in exactly this format (no extra text, no markdown):
{
  "points": [
    {
      "situation": "Brief context or background situation",
      "task": "The specific goal or challenge faced",
      "action": "What the candidate specifically did — be detailed",
      "result": "The measurable or meaningful outcome"
    }
  ]
}`;

    const response = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "x-api-key": apiKey,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
      },
      body: JSON.stringify({
        model: "claude-haiku-4-5-20251001",
        max_tokens: 1200,
        messages: [{ role: "user", content: prompt }],
      }),
    });

    if (!response.ok) {
      const err = await response.text();
      throw new Error(`Anthropic API error ${response.status}: ${err}`);
    }

    const claude = await response.json();
    const rawText: string = claude?.content?.[0]?.text ?? "";

    // Parse JSON — handle cases where Claude wraps it in markdown
    let points: unknown[] = [];
    try {
      const cleaned = rawText.replace(/^```(?:json)?\n?/, "").replace(/\n?```$/, "").trim();
      const parsed = JSON.parse(cleaned);
      points = Array.isArray(parsed?.points) ? parsed.points : [];
    } catch {
      // Try to extract JSON object from anywhere in the response
      const match = rawText.match(/\{[\s\S]*\}/);
      if (match) {
        try {
          const parsed = JSON.parse(match[0]);
          points = Array.isArray(parsed?.points) ? parsed.points : [];
        } catch {
          points = [];
        }
      }
    }

    return new Response(JSON.stringify({ points }), {
      headers: { ...CORS_HEADERS, "Content-Type": "application/json" },
    });
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    return new Response(JSON.stringify({ error: message }), {
      status: 500,
      headers: { ...CORS_HEADERS, "Content-Type": "application/json" },
    });
  }
});
