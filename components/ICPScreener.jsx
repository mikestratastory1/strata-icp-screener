import React, { useState, useRef, useEffect, useCallback } from 'react';
import { supabase, normalizeDomain, upsertCompany as dbUpsertCompany, createResearchRun, updateResearchRun, getCompaniesWithLatest, updateCompany as dbUpdateCompany, getTrainingExamples, createTrainingExample, updateTrainingExample, deleteTrainingExample, upsertContacts, upsertContact, updateContact, getAllContacts, getContactsByCompany, deleteContact, deleteCompany, getAllCampaigns, createCampaign, updateCampaign, deleteCampaign, getCampaignContacts, getAllCampaignContacts, addContactsToCampaign, removeContactFromCampaign, getCampaignMessages, createCampaignMessage, upsertCampaignMessage, deleteCampaignMessage, getSavedFilters, createSavedFilter, updateSavedFilter, deleteSavedFilter, getGeneratedMessages, upsertGeneratedMessages, updateGeneratedMessage, getGeneratedMessageContacts, getAllPrompts, upsertPrompt, deletePrompt } from '../lib/supabase';

const RESEARCH_PROMPT = `You are a B2B research analyst synthesizing pre-gathered data about a company. All the raw data from web searches, homepage crawls, news articles, competitor reviews, case studies, and social content has already been collected and is provided below. Your job is to organize this data into a structured research report. Do NOT make up information — only use what is in the provided data. If data for a field is missing, say so.

=== SECTION A: STRATEGIC RESEARCH ===

PRODUCT_SUMMARY: [What does the product do in 2-3 sentences. Not marketing language - describe it plainly like you're explaining to a colleague. Include the target market and key use cases.]

TARGET_CUSTOMER: [Who buys this? Company size (SMB/mid-market/enterprise), industries, geographic focus. Be specific - "Series B+ SaaS companies" not just "businesses." Use case studies, homepage logos, and LinkedIn description as evidence.]

TARGET_DECISION_MAKER: [Who is the primary buyer - the person who signs the contract, not the user. Look at case study data for who is quoted. Then infer ONE LEVEL UP to the budget holder. Example: if a Director of Content Marketing is quoted, the decision maker is the CMO or VP of Marketing. State the inferred decision maker title.]

TOP_3_OUTCOMES: [The 3 most specific outcomes this product helps the decision maker's team achieve. Must be outcomes with numbers/metrics when available. Example: "Reduce no-shows by 75%" not "appointment reminders." Pull from case studies and news data.

For each outcome, provide:
- Outcome: the specific result with numbers/metrics
- Source: where you found this (e.g. "AT&T case study on homepage", "G2 review by enterprise user", "press release Jan 2025")
- Explanation: one sentence explaining what the source said
- Confidence: High (multiple sources or named case study with specific metrics), Medium (single source with some specifics), Low (inferred from marketing copy or vague references)

Format as:
1. [Outcome] | Source: [source] | Explanation: [one sentence] | Confidence: [High/Medium/Low]
2. [Outcome] | Source: [source] | Explanation: [one sentence] | Confidence: [High/Medium/Low]
3. [Outcome] | Source: [source] | Explanation: [one sentence] | Confidence: [High/Medium/Low]]

TOP_3_DIFFERENTIATORS: [What 3 things make this company different from competitors? Use the competitor comparison data and reviews. IMPORTANT: deprioritize "easy to use", "great UX", "simple interface" — these are not meaningful differentiators. Look for capability themes, unique approaches, integration advantages, or specific outcomes. Each differentiator should pass the test: "Could a competitor also say this?" If yes, it's not a differentiator.

For each differentiator, provide:
- Differentiator: the specific capability or approach
- Source: where you found this (e.g. "G2 reviews comparing to Datadog", "TechCrunch article Dec 2024", "competitor page comparison")
- Explanation: one sentence explaining what the source said that supports this claim
- Confidence: High (multiple independent sources confirm this is unique), Medium (one strong source or multiple weak ones), Low (primarily from company's own marketing with no third-party validation)

Format as:
1. [Differentiator] | Source: [source] | Explanation: [one sentence] | Confidence: [High/Medium/Low]
2. [Differentiator] | Source: [source] | Explanation: [one sentence] | Confidence: [High/Medium/Low]
3. [Differentiator] | Source: [source] | Explanation: [one sentence] | Confidence: [High/Medium/Low]]

MAJOR_ANNOUNCEMENTS: [ALL major product launches, acquisitions, partnerships, pivots, rebrands, and new market entries from the news data. Include dates. Exclude funding rounds unless they accompanied a product/market change. If no major announcements found, write "None found."]

COMPETITORS: [Name each major competitor (3-5) from the comparison data. For each, state one sentence on what capability THIS company has that THAT competitor lacks.]

=== SECTION B: COMPANY FACTS ===

COMPANY_CUSTOMERS: [Named customers from case studies, homepage content, news articles]
COMPANY_FUNDING: [Each round: date, amount, lead investors. From funding data.]
COMPANY_TEAM_SIZE: [Approximate headcount from LinkedIn data or news]

=== SECTION C: HOMEPAGE & PRODUCT PAGE CONTENT ===

Use the homepage content crawled via Exa. This is clean markdown extracted from the live page.

RAW_HOMEPAGE_CONTENT: [Reproduce the homepage content IN FULL verbatim - every word from the crawled data. This is critical, the scoring step reads this directly. If not available, write "NOT AVAILABLE."]

HOMEPAGE_SECTIONS: [Break the homepage into sequential sections as they appear top-to-bottom on the page. Each section is a distinct visual block (hero, features, social proof, testimonials, CTA, etc.). For each section, capture the copy verbatim. Format as:

SECTION 1 (Hero): [Exact headline, subheadline, and any supporting text in the hero area. This is the most important section.]
SECTION 2: [Next visual block below the hero - could be logos, a value prop section, a features grid, etc. Include a brief label of what it is, then the copy.]
SECTION 3: [Next block]
SECTION 4: [Next block]
...continue for all sections on the page.

Capture ALL text content in each section including headlines, body copy, button text, testimonial quotes, metric callouts, and badge/label text. The scoring step uses this to evaluate messaging quality with decreasing weight from top to bottom.]

HOMEPAGE_NAVIGATION: [All main nav items from the homepage content. Note whether organized by product names or buyer problems. List any product-specific subnav items.]

HOMEPAGE_BANNERS_AND_LINKS: [Any promotional banners, "What's New" links, announcement links visible in the homepage content.]

PRODUCT_PAGES: [For EACH distinct product/subpage crawled, capture: product name, hero headline, key value prop, implied audience, and implied use case. Format as:
- Product 1: [name] | Hero: [headline] | Value prop: [key claim] | Audience: [who it's for]
- Product 2: [name] | Hero: [headline] | Value prop: [key claim] | Audience: [who it's for]
If there is only one product (no separate product pages in the data), write "Single product - no separate product pages."]

NEW_DIRECTION_PAGE: [If the news data reveals a recent change (acquisition, pivot, new product), identify the best piece of content describing the new direction and reproduce its key message (up to 2000 chars). If no recent change, write "N/A."]

=== SECTION D: LINKEDIN ===

LINKEDIN_COMPANY_DESCRIPTION: [From the LinkedIn data provided, extract and reproduce the company About section verbatim. If not available, write "Not found."]

=== SECTION E: CEO/FOUNDER VOICE ===

CEO_FOUNDER_NAME: [From tweets and CEO content data, identify the CEO or Founder. Report: Name, Title.]

CEO_RECENT_CONTENT: [From the tweets and CEO blog/podcast/conference data, capture up to 5 pieces of content. For each:
- Source (tweet, blog, podcast, conference, etc.)
- Date (approximate)
- Key message in 1-2 sentences: what narrative is the CEO pushing?
If no recent content found, write "None found."]

CEO_NARRATIVE_THEME: [Based on the CEO content above, what is the CEO's current narrative theme in 1-2 sentences? How does this compare to the homepage messaging?]

=== SECTION F: PEOPLE SEARCH ===

NEW_MARKETING_LEADER: [From any of the provided data, identify if there is a VP of Marketing, CMO, or Head of Marketing who joined in the last 12 months. Report: Name, Title, ~Start Date. If not found, write "None found."]

PRODUCT_MARKETING_PEOPLE: [From any of the provided data, identify product marketing people. Report: Name (Title, ~Start Date) for each. If not found, write "None found."]`;


// === INDIVIDUAL FACTOR SCORING PROMPTS (Haiku) ===
const SYSTEM_PROMPT_SCORER = "You are a JSON-only scoring API. Respond with a single valid JSON object. No markdown, no code fences, no text before or after. Start with { and end with }.";

const DISQUALIFICATION_PROMPT = `Check if this company should be disqualified from B2B SaaS narrative gap scoring. Return JSON: {"disqualified": true/false, "reason": "explanation or None"}

Disqualify if ANY apply:
- Acquired by larger company (not independent)
- Consumer product, not B2B SaaS
- Crypto/Web3/prediction markets
- Pre-product or research-phase`;

const FACTOR_A_PROMPT = `Score this company's DIFFERENTIATION gap (Factor A). Score 1-3, higher = larger gap.

The research below describes technical capabilities. Your job is to restate them as the unique BENEFIT or VALUE a customer gets. Then check whether the homepage communicates those benefits clearly. If the CEO describes the unique value in their own words, prefer their language.

+1: Hero or early sections communicate specific value that competitors can't claim.
+2: Differentiators exist but the hero is generic. Unique value appears in later sections or uses technical language that dilutes the benefit.
+3: Homepage could belong to any competitor. Does not clearly communicate unique value.

Return JSON:
{
  "score": 2,
  "differentiators": ["Customer benefit 1 in plain language", "Customer benefit 2", "Customer benefit 3"],
  "homepage_sections": [
    {"name": "Hero", "finding": "Quote specific copy", "status": "hit or miss"},
    {"name": "Section name", "finding": "Quote specific copy", "status": "hit or miss"}
  ],
  "verdict": "One sentence."
}

status: "hit" = benefit clearly communicated, "miss" = absent, generic, or too technical. Include the first 4 content sections.`;

const FACTOR_B_PROMPT = `Score this company's OUTCOMES gap (Factor B). Score 1-3, higher = larger gap.

Focus on key KPIs the target decision maker cares about most. List each as a short KPI name only (e.g. "Reduce inventory costs" not "Reduce inventory by 10-15%"). If the CEO talks about outcomes, use their framing.

+1: Homepage prominently features quantified strategic outcomes (cost, revenue, risk, margin) in Sections 1-2
+2: Outcomes exist but are tactical, buried below Section 2, or lack metrics
+3: Dominated by features — outcomes absent or vague

Return JSON:
{
  "score": 2,
  "decision_maker": "Title of primary buyer",
  "outcomes": ["Short KPI name", "Short KPI name", "Short KPI name"],
  "homepage_sections": [
    {"name": "Hero", "finding": "Quote specific copy", "outcome_type": "strategic, tactical, or none"},
    {"name": "Section name", "finding": "Quote specific copy", "outcome_type": "strategic, tactical, or none"}
  ],
  "verdict": "One sentence."
}

Include the first 4 content sections.`;

const FACTOR_C_PROMPT = `Score this company's CUSTOMER-CENTRIC gap (Factor C). Score 1-3, higher = larger gap.

Evaluate company-authored copy ONLY — exclude testimonials. Key question: is value framed from the buyer's perspective? A sentence can mention the product and still be customer-centric if framed as what the buyer gets. Imperative verbs ("Empower your team") are customer-centric. Product-centric means the product is the hero ("Our platform delivers").

+1: Hero and primary sections frame value from the buyer's perspective
+2: Mixed — some buyer-oriented language but significant sections default to product-as-hero
+3: Homepage primarily talks about what the product is and does

Return JSON:
{
  "score": 2,
  "sections": [
    {"name": "Hero", "orientation": "product-centric, customer-centric, mixed, or excluded", "evidence": "Quote the specific copy"},
    {"name": "Section name", "orientation": "...", "evidence": "Quote the specific copy"}
  ],
  "verdict": "One sentence."
}

Include the first 4 content sections. Use "excluded" for testimonial sections.`;

const FACTOR_D_PROMPT = `Score this company's PRODUCT CHANGE gap (Factor D). Score 1-3, higher = larger gap.

Evaluate how much the product has changed AND whether the homepage reflects the new direction. Use the CEO's vision and language to describe the "after" state — how they describe where the company is going, not a technical product description. The homepage does not need to match the CEO's visionary tone exactly to score well.

+1: Homepage messaging is similar to the new direction, even if there are small differences.
+2: Either (a) product transformed but homepage has some similarities and needs work, OR (b) value prop is stretching but homepage is missing most of the new story.
+3: Product transformed and homepage is missing most of the new story.

Return JSON:
{
  "score": 2,
  "changes": [
    {"date": "Month YYYY", "name": "Change name", "before": "What the company story used to be about", "after": "Where the company is heading in the CEO's words"}
  ],
  "homepage_alignment": "Quote specific homepage copy and assess how well it reflects the new direction.",
  "verdict": "One sentence."
}

Empty array [] for changes if no significant product changes.`;

const FACTOR_E_PROMPT = `Score this company's AUDIENCE CHANGE gap (Factor E). Score 1-3, higher = larger gap.

Has the target buyer or market shifted? Include a confidence rating.

+1: Buyer and market consistent 12+ months
+2: Expanding into adjacent segment or secondary persona
+3: Meaningful shift in who they sell to

Return JSON:
{
  "score": 1,
  "before": {"buyer": "Title", "department": "Dept", "market": "Market segment"},
  "today": {"buyer": "Title", "department": "Dept", "market": "Market segment"},
  "confidence": "High, Medium, or Low",
  "confidence_reason": "One sentence citing the specific source.",
  "verdict": "One sentence."
}

High = explicit announcement naming new audience. Medium = indirect signals. Low = inferred.`;

const FACTOR_F_PROMPT = `Score this company's MULTI-PRODUCT gap (Factor F). Score 1-3, higher = larger gap.

Does the company have multiple products that create a fragmented narrative?

+1: Single product or tightly integrated suite, unified narrative
+2: Multiple products but homepage connects them under one story
+3: Products have different audiences/value props — a visitor would be confused

Return JSON:
{
  "score": 1,
  "products": [{"name": "Product name", "tag": "module, product, or suite"}],
  "visitor_experience": "One sentence on whether a homepage visitor sees a unified story or feels confused.",
  "verdict": "One sentence."
}`;

const FACTOR_G_PROMPT = `Score this company's VISION GAP (Factor G). Score 1-3, higher = larger gap.

Compare what the CEO says publicly vs what the homepage says. Quote or closely paraphrase specific language from both so the contrast is visible.

+1: CEO and homepage tell a similar story, largely aligned even if not identical.
+2: CEO is ahead of the homepage — emphasizes a direction the homepage hints at but doesn't commit to.
+3: CEO is telling a fundamentally different story than the homepage.

Return JSON:
{
  "score": 2,
  "ceo_narrative": "Quote or closely paraphrase the CEO's specific language.",
  "homepage_narrative": "Quote or closely paraphrase the homepage's specific language.",
  "verdict": "One sentence describing the disconnect or alignment."
}`;

const FACTOR_NAMES = { A: 'Differentiation', B: 'Outcomes', C: 'Customer-centric', D: 'Product Change', E: 'Audience Change', F: 'Multi-product', G: 'Vision Gap' };
const GAP_TIEBREAKER = ['G', 'D', 'E', 'B', 'A', 'C', 'F'];

// === GAP-SPECIFIC OBSERVATION PROMPTS ===
// Each prompt tells Sonnet how to turn a specific gap type into a compelling email opening.
// Variables injected at runtime: companyName, firstName, contactTitle, verdict, heroText

const OBS_PROMPT_A = `<role>You are the founder of a strategic narrative consultancy writing a cold email opening (60-75 words) to the founder of a company.</role>

<goal>
The goal of the opening is to make the founder feel you really understand their core anxiety: being lumped in with competitors whose product can't do what their company can do.
</goal>

<task>
Write a 3-5 sentence email opening that hits these 5 beats in order:

1. ARE YOU BEING LUMPED IN WITH COMPETITORS? Ask whether the company's target buyers see them as interchangeable with the existing category. This names the founder's core fear. Can be a question ("Are supply chain leaders treating ketteQ like just another planning vendor?") or a bold statement ("Renderfast shouldn't be compared to generic deployment platforms.").

2. WHAT COMPETITORS ARE DOING. Establish that you did real research by referencing specific competitors (if confidence is high) or the general category (if low). Name what competitors are still doing the old way. This sets up the contrast.

3. YOU'RE DIFFERENT. State the company's differentiator in plain, vivid language. Do not parrot the exact phrasing from the inputs. Make it concrete and memorable. This can be a standalone punch ("You're not.") or a longer statement. Note: beats 2 and 3 can merge into one sentence when it flows better.

4. BASED ON YOUR HOMEPAGE, THERE'S AN OPPORTUNITY. Reference the homepage as evidence of a growth opportunity they could achieve by featuring their differentiator more prominently. Always framed as untapped potential, never as a critique.

5. IS IT LANDING IN SALES? Ask whether the same opportunity might exist in the sales story. Is their differentiation getting across in sales? This flows directly from the homepage observation as one connected thought. Short, casual, ends the paragraph cleanly. Note: beats 4 and 5 can merge into one sentence when it flows better.

The total should be 3-5 sentences covering all 5 beats. The email must flow as one natural paragraph. It should sound like a peer who did some research and is sharing what they found, not like a consultant running a playbook. Every email should feel like it was written specifically for this company.
</task>

The <context> block below contains the company data. Use it as your source material.
The <examples> block contains openings that set the standard. Study the <why_it_works> annotations. Your output should feel like it belongs in this set.

<rules>
- 60-75 words. Hard limit.
- Casual, direct, peer-level tone.
- Do not tell the reader something about their company or market without prefacing it with a point about the work we put in to learn it. We don't want to come across as presumptive.
- Frame homepage as opportunity, never criticism.
- No dashes as punctuation.
- This is the opening paragraph only. A credibility block and CTA follow by code. End clean.
</rules>`;

const OBS_PROMPT_B = `You are writing a cold email opening about an OUTCOMES gap.

The company has strong customer outcomes (cost savings, revenue impact, risk reduction) but the homepage leads with features and capabilities instead of the strategic results buyers care about. The decision maker wants to see business impact, not product specs.

Your observation should contrast the strategic outcomes the company actually delivers (from the verdict) vs the feature-led language on the homepage. The opportunity is to lead with what buyers actually care about in buying decisions.

Example angle: "Noticed your customers are seeing [type of outcome], but your site leads with [feature/capability language] before getting to the impact."`;

const OBS_PROMPT_C = `You are writing a cold email opening about a CUSTOMER-CENTRIC gap.

The homepage talks about what the product does rather than what the buyer achieves. The copy puts the product as the hero of every sentence instead of the buyer's world, goals, and problems.

Your observation should contrast the product-as-hero framing on the homepage vs how the story could start from the buyer's perspective. The opportunity is to reframe around the buyer's situation.

Example angle: "Noticed your site leads with what [product] does, but the buyers you're targeting are probably starting from [their problem/goal]."`;

const OBS_PROMPT_D = `You are writing a cold email opening about a PRODUCT CHANGE gap.

The company's product has evolved significantly (new capabilities, new direction, new positioning from the CEO), but the homepage still tells the old story. Someone reading the homepage would get a materially different understanding of what the company has become.

Your observation should contrast the CEO's vision or the company's new direction (from the verdict) vs the outdated story on the homepage. The opportunity is to bring the homepage up to speed with where the company actually is.

Example angle: "Noticed your CEO is talking about [new direction], but your site still opens with [old positioning]. Feels like there's a bigger story to tell."`;

const OBS_PROMPT_E = `You are writing a cold email opening about an AUDIENCE CHANGE gap.

The company is expanding into new buyer segments or has shifted who they sell to, but the homepage narrative still speaks to the original audience. The new buyers may not see themselves in the current story.

Your observation should contrast who the company is actually selling to now (from the verdict) vs who the homepage seems to address. The opportunity is to make the new audience feel like the product was built for them.

Example angle: "Noticed you're moving into [new segment/buyer], but your site still reads like it's written for [original audience]."`;

const OBS_PROMPT_F = `You are writing a cold email opening about a MULTI-PRODUCT gap.

The company has multiple products or modules, but the homepage doesn't tell a unified story. A visitor would be confused about what the company actually does or which product is for them.

Your observation should note the fragmented narrative across products (from the verdict) vs the opportunity to tell one cohesive story. The opportunity is to connect the products under a single compelling narrative.

Example angle: "Noticed you have [product A] and [product B], but your site presents them as separate stories. There might be a bigger narrative that ties them together."`;

const OBS_PROMPT_G = `You are writing a cold email opening about a VISION GAP.

The CEO is telling a fundamentally different (and usually more ambitious) story than what the homepage says. The CEO's public narrative describes the company's mission, value, or direction in ways that would surprise someone who only read the homepage.

Your observation should quote or paraphrase the contrast between the CEO's language and the homepage's language (from the verdict). The opportunity is to bring the CEO's vision into the homepage so buyers see the bigger story immediately.

Example angle: "Noticed your CEO describes the company as [CEO's framing], but your site opens with [homepage's framing]. The CEO story is more compelling."`;

const DEFAULT_OBS_PROMPTS = { A: OBS_PROMPT_A, B: OBS_PROMPT_B, C: OBS_PROMPT_C, D: OBS_PROMPT_D, E: OBS_PROMPT_E, F: OBS_PROMPT_F, G: OBS_PROMPT_G };

// Shared rules appended to every observation prompt
const OBS_SHARED_RULES = `Rules:
- No specific metrics, dollar amounts, or percentages
- No em-dashes. Use periods instead.
- No adjective stacking (max one adjective before a noun)
- Banned words: comprehensive, ecosystem, holistic, end-to-end, cutting-edge, innovative, revolutionary, robust, seamless, positions, articulates, communicates, demonstrates
- No homepage design language (fold, hero section, sections, page layout)
- Frame POSITIVELY. Describe what the company does well and where there is potential. Do not criticize.`;

// Default email template parts
const DEFAULT_EMAIL_TEMPLATES = {
  credibility_adg: 'I run a narrative & product marketing consultancy and recently helped another client, Zime, tell a new differentiated AI story. After 9 months, revenue grew 400%.',
  credibility_bc: 'I run a narrative & product marketing consultancy and recently helped another client, BugZero, reframe their AI story around buyer impact. After 6 months, sales cycles dropped 80%.',
  cta: 'As a starting point, I offer a free story gap assessment that could surface a few ways your story could better land with {{buyerTitle}}.',
  email2_closing: 'I specialize in helping companies like yours tell a sharper GTM story. Want me to put together a quick assessment?',
  signoff: 'Mike',
};

// === PANEL COMPONENTS (matching mockup layout) ===

const SL = ({ name }) => (
  <span className="inline-block text-[9px] font-semibold text-gray-500 bg-gray-100 rounded px-1.5 py-px mr-1.5 border border-gray-700 whitespace-nowrap">{name}</span>
);

const Tag = ({ children, color = 'gray' }) => {
  const colors = {
    red: 'text-red-600 bg-red-50 border-red-400/20',
    green: 'text-green-600 bg-green-50 border-green-200',
    amber: 'text-amber-600 bg-amber-50 border-amber-400/20',
    gray: 'text-gray-500 bg-gray-500/10 border-gray-500/20',
    purple: 'text-purple-400 bg-purple-400/10 border-purple-400/20',
    cyan: 'text-cyan-400 bg-cyan-400/10 border-cyan-400/20',
  };
  return <span className={`inline-block text-[9px] px-1.5 py-px rounded border mr-1 ${colors[color] || colors.gray}`}>{children}</span>;
};

const Lbl = ({ children }) => (
  <div className="text-gray-500 font-semibold mb-1 text-[9px] uppercase tracking-wider">{children}</div>
);

const Verdict = ({ color = 'gray', children }) => {
  const colors = { red: 'text-red-600', green: 'text-green-600', amber: 'text-amber-600', gray: 'text-gray-500' };
  return <div className={`${colors[color] || colors.gray} text-[11px] mt-1.5 font-medium`}>→ {children}</div>;
};

function tryParseJSON(str) {
  if (!str) return null;
  try { return typeof str === 'object' ? str : JSON.parse(str); } catch { return null; }
}

function verdictColor(score) {
  return score === 3 ? 'green' : score === 2 ? 'amber' : 'red';
}

function PanelA({ d }) {
  return (
    <>
      <div className="grid grid-cols-2 gap-2.5 text-[11px]">
        <div>
          <Lbl>Research: Differentiators</Lbl>
          <div className="flex flex-col gap-0.5 text-gray-400">
            {(d.differentiators || []).map((diff, i) => (
              <div key={i}><span className="text-purple-400">{i + 1}.</span> {diff}</div>
            ))}
          </div>
        </div>
        <div>
          <Lbl>Homepage by Section</Lbl>
          <div className="flex flex-col gap-0.5 text-gray-500">
            {(d.homepage_sections || []).map((s, i) => (
              <div key={i}>
                <SL name={s.name} />
                <span className={s.status === 'hit' ? 'text-green-600' : 'text-red-600'}>{s.finding}</span>
              </div>
            ))}
          </div>
        </div>
      </div>
      {d.verdict && <Verdict color={verdictColor(d.score)}>{d.verdict}</Verdict>}
    </>
  );
}

function PanelB({ d }) {
  return (
    <>
      <div className="grid grid-cols-2 gap-2.5 text-[11px]">
        <div>
          <Lbl>Decision Maker & Key Outcomes</Lbl>
          {d.decision_maker && <div className="text-gray-400 mb-1 font-semibold">{d.decision_maker}</div>}
          {(d.outcomes || d.strategic_outcomes || []).length > 0 && (
            <div className="flex flex-col gap-px text-gray-500 mb-1.5">
              {(d.outcomes || [...(d.strategic_outcomes || []), ...(d.tactical_outcomes || [])]).map((o, i) => <div key={i}>• {o}</div>)}
            </div>
          )}
        </div>
        <div>
          <Lbl>Homepage by Section</Lbl>
          <div className="flex flex-col gap-1 text-gray-500">
            {(d.homepage_sections || []).map((s, i) => (
              <div key={i}>
                <SL name={s.name} />
                <span className={s.outcome_type === 'strategic' ? 'text-rose-600' : s.outcome_type === 'tactical' ? 'text-amber-600' : 'text-red-600'}>
                  {s.finding}
                </span>
                {s.outcome_type !== 'none' && <Tag color={s.outcome_type === 'strategic' ? 'red' : 'amber'}>{s.outcome_type}</Tag>}
              </div>
            ))}
          </div>
        </div>
      </div>
      {d.verdict && <Verdict color={verdictColor(d.score)}>{d.verdict}</Verdict>}
    </>
  );
}

function PanelC({ d }) {
  const orientColors = { 'product-centric': 'red', 'customer-centric': 'green', 'mixed': 'amber', 'excluded': 'gray' };
  return (
    <>
      <Lbl>Language Orientation by Section</Lbl>
      <div className="text-[10px] text-gray-500 mb-1.5 italic">Evaluates company's own copy. Testimonials/quotes excluded from scoring.</div>
      <div className="flex flex-col gap-1 text-[11px]">
        {(d.sections || []).map((s, i) => (
          <div key={i} className="text-gray-500">
            <SL name={s.name} />
            <Tag color={orientColors[s.orientation] || 'gray'}>{s.orientation}</Tag>
            <span className="text-gray-500">{s.evidence}</span>
          </div>
        ))}
      </div>
      {d.verdict && <Verdict color={verdictColor(d.score)}>{d.verdict}</Verdict>}
    </>
  );
}

function PanelD({ d }) {
  return (
    <>
      <Lbl>Product Changes (Last 24 Months)</Lbl>
      {(d.changes || []).length === 0 ? (
        <div className="text-[11px] text-gray-500">No significant product changes found.</div>
      ) : (
        <div className="flex flex-col gap-3 text-[11px]">
          {d.changes.map((ch, i) => (
            <div key={i}>
              <div className="flex items-center gap-1.5 mb-1.5">
                <span className="text-cyan-600 font-semibold">{ch.name}</span>
                <span className="text-gray-400 text-[10px]">{ch.date}</span>
              </div>
              <div className="grid grid-cols-2 gap-2 mb-2">
                <div className="p-2 bg-gray-50 rounded border border-gray-200">
                  <div className="text-[9px] text-gray-400 font-semibold mb-1">BEFORE</div>
                  <div className="text-gray-700 leading-relaxed">{ch.before}</div>
                </div>
                <div className="p-2 bg-emerald-50 rounded border border-emerald-200">
                  <div className="text-[9px] text-emerald-600 font-semibold mb-1">NEW DIRECTION</div>
                  <div className="text-gray-700 leading-relaxed">{ch.after}</div>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
      {d.homepage_alignment && (
        <div className="mt-2 p-2 bg-amber-50/60 rounded border border-amber-200/60">
          <div className="text-[9px] text-amber-600 font-semibold mb-1">HOMEPAGE VS NEW DIRECTION</div>
          <div className="text-[11px] text-gray-700 leading-relaxed">{d.homepage_alignment}</div>
        </div>
      )}
      {d.verdict && <Verdict color={verdictColor(d.score)}>{d.verdict}</Verdict>}
    </>
  );
}

function PanelE({ d }) {
  return (
    <>
      <div className="grid grid-cols-2 gap-2.5 text-[11px]">
        <div>
          <Lbl>12+ Months Ago</Lbl>
          {d.before && (
            <>
              <div className="text-gray-400">{d.before.buyer}</div>
              <div className="text-gray-500">{d.before.department}</div>
              <div className="text-gray-500">{d.before.market}</div>
            </>
          )}
        </div>
        <div>
          <Lbl>Today</Lbl>
          {d.today && (
            <>
              <div className="text-gray-400">{d.today.buyer}</div>
              <div className="text-gray-500">{d.today.department}</div>
              <div className="text-gray-500">{d.today.market}</div>
            </>
          )}
        </div>
      </div>
      {d.verdict && <Verdict color={verdictColor(d.score)}>{d.verdict}</Verdict>}
    </>
  );
}

function PanelF({ d }) {
  const tagColors = { module: 'purple', product: 'cyan', suite: 'amber' };
  return (
    <>
      <Lbl>Product Architecture</Lbl>
      {(d.products || []).length > 0 && (
        <div className="flex gap-1 flex-wrap mb-1">
          {d.products.map((p, i) => (
            <Tag key={i} color={tagColors[p.tag] || 'gray'}>{p.name}</Tag>
          ))}
        </div>
      )}
      {(d.visitor_experience || d.description) && <div className="text-[11px] text-gray-500 mb-1">{d.visitor_experience || d.description}</div>}
      {d.verdict && <Verdict color={verdictColor(d.score)}>{d.verdict}</Verdict>}
    </>
  );
}

function Collapsible({ title, children }) {
  const [open, setOpen] = useState(false);
  return (
    <div>
      <div onClick={() => setOpen(!open)} className="text-[11px] text-gray-500 cursor-pointer font-medium py-0.5 select-none hover:text-gray-400">{open ? '▾' : '▸'} {title}</div>
      {open && <div className="mt-1 p-2.5 bg-gray-100 rounded-md text-[11px] text-gray-400 flex flex-col gap-1">{children}</div>}
    </div>
  );
}

function ResearchReportField({ label, value }) {
  if (!value || value === 'None found.' || value === 'N/A' || value === 'NOT AVAILABLE.') return null;
  return (
    <div className="mb-2">
      <div className="text-[10px] text-gray-500 font-semibold uppercase tracking-wider mb-0.5">{label}</div>
      <div className="text-[11px] text-gray-700 whitespace-pre-wrap leading-relaxed">{value}</div>
    </div>
  );
}

function ResearchReport({ company }) {
  const c = company;
  if (!c || !c.researchResult) return (
    <div className="text-center py-8 text-gray-400 text-[12px]">No research data available. Screen this account to generate a report.</div>
  );

  const sections = [
    {
      title: 'Strategic Research',
      fields: [
        { label: 'Product Summary', value: c.productSummary },
        { label: 'Target Customer', value: c.targetCustomer },
        { label: 'Target Decision Maker', value: c.targetDecisionMaker },
        { label: 'Top 3 Outcomes', value: c.top3Outcomes },
        { label: 'Top 3 Differentiators', value: c.top3Differentiators },
        { label: 'Major Announcements', value: c.majorAnnouncements },
        { label: 'Competitors', value: c.competitors },
      ]
    },
    {
      title: 'Company Facts',
      fields: [
        { label: 'Customers', value: c.customers },
        { label: 'Funding', value: c.funding },
        { label: 'Team Size', value: c.teamSize },
      ]
    },
    {
      title: 'Homepage & Product Pages',
      fields: [
        { label: 'Homepage Sections', value: c.homepageSections },
        { label: 'Homepage Navigation', value: c.homepageNav },
        { label: 'Product Pages', value: c.productPages },
        { label: 'New Direction Page', value: c.newDirectionPage },
      ]
    },
    {
      title: 'LinkedIn',
      fields: [
        { label: 'Company Description', value: c.linkedinDescription },
      ]
    },
    {
      title: 'CEO / Founder Voice',
      fields: [
        { label: 'CEO / Founder', value: c.ceoFounderName },
        { label: 'Recent Content', value: c.ceoRecentContent },
        { label: 'Narrative Theme', value: c.ceoNarrativeTheme },
      ]
    },
    {
      title: 'People',
      fields: [
        { label: 'New Marketing Leader', value: c.newMarketingLeader },
        { label: 'Product Marketing Team', value: c.productMarketingPeople },
      ]
    },
  ];

  return (
    <div className="flex flex-col gap-0.5">
      {sections.map((section, si) => {
        const hasContent = section.fields.some(f => f.value && f.value !== 'None found.' && f.value !== 'N/A' && f.value !== 'NOT AVAILABLE.');
        if (!hasContent) return null;
        return (
          <Collapsible key={si} title={`Research: ${section.title}`}>
            <div className="flex flex-col gap-0">
              {section.fields.map((f, fi) => (
                <ResearchReportField key={fi} label={f.label} value={f.value} />
              ))}
            </div>
          </Collapsible>
        );
      })}
      <Collapsible title="Raw: Research Output">
        <pre className="text-[10px] text-gray-500 leading-relaxed whitespace-pre-wrap max-h-64 overflow-auto">{c.researchResult}</pre>
      </Collapsible>
      <Collapsible title="Raw: Scoring Output">
        <pre className="text-[10px] text-gray-500 leading-relaxed whitespace-pre-wrap max-h-64 overflow-auto">{c.scoringResult}</pre>
      </Collapsible>
    </div>
  );
}

function PanelG({ d }) {
  return (
    <>
      {d.ceo_narrative && (
        <>
          <Lbl>CEO Narrative</Lbl>
          <div className="text-[11px] text-gray-700 mb-2 leading-relaxed">{d.ceo_narrative}</div>
        </>
      )}
      {d.homepage_narrative && (
        <>
          <Lbl>Homepage Narrative</Lbl>
          <div className="text-[11px] text-gray-500 mb-2 leading-relaxed">{d.homepage_narrative}</div>
        </>
      )}
      {d.verdict && <Verdict color={verdictColor(d.score)}>{d.verdict}</Verdict>}
    </>
  );
}

const PANEL_MAP = { A: PanelA, B: PanelB, C: PanelC, D: PanelD, E: PanelE, F: PanelF, G: PanelG };

function FactorPanel({ factorKey, data }) {
  if (!data) return null;
  const d = tryParseJSON(data);
  if (d && typeof d === 'object' && (d.verdict || d.sections || d.differentiators || d.changes || d.products || d.before || d.homepage_sections || d.outcomes || d.strategic_outcomes || d.tactical_outcomes || d.decision_maker || d.visitor_experience || d.description || d.ceo_narrative || d.homepage_narrative || d.gap)) {
    const Panel = PANEL_MAP[factorKey];
    if (Panel) return <Panel d={d} />;
  }
  // Fallback: render as plain text (for old-format justifications)
  const text = typeof data === 'string' ? data : JSON.stringify(data);
  // If it looks like raw JSON that didn't match, try to show verdict at minimum
  if (d && d.verdict) return <div className="text-[11px] text-gray-500 leading-relaxed">→ {d.verdict}</div>;
  return <div className="text-[11px] text-gray-500 leading-relaxed">{text}</div>;
}


function CampaignMessageCard({ msg, onSave, onDelete }) {
  const [subject, setSubject] = useState(msg.subject || '');
  const [body, setBody] = useState(msg.body || '');
  const isDirty = subject !== (msg.subject || '') || body !== (msg.body || '');
  const isEmail = msg.channel === 'email';

  return (
    <div className="bg-white border border-gray-200 rounded-lg overflow-hidden">
      <div className="flex items-center justify-between px-4 py-2.5 border-b border-gray-100">
        <div className="flex items-center gap-2">
          <span className={`w-2.5 h-2.5 rounded-sm ${isEmail ? 'bg-violet-400' : 'bg-sky-400'}`} />
          <span className="text-[13px] font-semibold text-gray-900">{isEmail ? 'Email' : 'LinkedIn'} #{msg.step_number}</span>
          <span className="text-[11px] text-gray-400">Step {msg.step_number}</span>
        </div>
        <div className="flex items-center gap-2">
          <button onClick={() => onSave({ ...msg, subject, body })}
            className={`px-3 py-1 rounded-lg text-xs font-medium border transition-colors ${
              isDirty ? 'bg-violet-600 text-white border-violet-600 hover:bg-violet-500' : 'bg-white text-gray-400 border-gray-200'
            }`}>
            Save
          </button>
          <button onClick={() => onDelete(msg.id)} className="text-gray-300 hover:text-red-500 text-sm transition-colors">🗑</button>
        </div>
      </div>
      <div className="px-4 py-3 space-y-2">
        {isEmail && (
          <input type="text" placeholder="Subject line..." value={subject}
            onChange={e => setSubject(e.target.value)}
            className="w-full bg-transparent border-b border-gray-100 pb-2 text-[13px] text-gray-700 placeholder-gray-400 outline-none focus:border-violet-300 transition-colors" />
        )}
        <textarea placeholder={isEmail ? "Write your email body..." : "Write your LinkedIn message..."}
          value={body} onChange={e => setBody(e.target.value)}
          className="w-full bg-transparent text-[13px] text-gray-700 placeholder-gray-400 outline-none resize-y min-h-[100px]"
          rows={5} />
      </div>
    </div>
  );
}

function GeneratedMessageCard({ msg, onSave, onSaveExample }) {
  const [subject, setSubject] = useState(msg.subject || '');
  const [body, setBody] = useState(msg.body || '');
  const [savedAsExample, setSavedAsExample] = useState(false);
  const isDirty = subject !== (msg.subject || '') || body !== (msg.body || '');
  const isEmail = msg.channel === 'email';

  useEffect(() => {
    setSubject(msg.subject || '');
    setBody(msg.body || '');
    setSavedAsExample(false);
  }, [msg.id, msg.subject, msg.body]);

  return (
    <div className="bg-white border border-gray-200 rounded-lg overflow-hidden">
      <div className="flex items-center justify-between px-4 py-2.5 border-b border-gray-100">
        <div className="flex items-center gap-2">
          <span className={`w-2.5 h-2.5 rounded-sm ${isEmail ? 'bg-violet-400' : 'bg-sky-400'}`} />
          <span className="text-[13px] font-semibold text-gray-900">{isEmail ? 'Email' : 'LinkedIn'} #{msg.step_number}</span>
        </div>
        <div className="flex items-center gap-2">
          {isDirty && (
            <button onClick={() => onSave({ ...msg, subject, body })}
              className="px-3 py-1 rounded-lg text-xs font-medium bg-violet-600 text-white border border-violet-600 hover:bg-violet-500">
              Save Edit
            </button>
          )}
          {onSaveExample && !savedAsExample && (
            <button onClick={() => { onSaveExample({ ...msg, subject, body }); setSavedAsExample(true); }}
              className="px-3 py-1 rounded-lg text-xs font-medium bg-emerald-50 text-emerald-700 border border-emerald-200 hover:bg-emerald-100">
              Save as Example
            </button>
          )}
          {savedAsExample && <span className="text-[10px] text-emerald-600">✓ Saved</span>}
        </div>
      </div>
      <div className="px-4 py-3 space-y-2">
        {isEmail && (
          <input type="text" placeholder="Subject line..." value={subject}
            onChange={e => setSubject(e.target.value)}
            className="w-full bg-transparent border-b border-gray-100 pb-2 text-[13px] text-gray-700 placeholder-gray-400 outline-none focus:border-violet-300 transition-colors" />
        )}
        <textarea placeholder={isEmail ? "Email body..." : "LinkedIn message..."}
          value={body} onChange={e => setBody(e.target.value)}
          className="w-full bg-transparent text-[13px] text-gray-700 placeholder-gray-400 outline-none resize-y min-h-[80px]"
          rows={5} />
      </div>
    </div>
  );
}

export default function ICPScreener() {
  const [companies, setCompanies] = useState([]);
  const [isProcessing, setIsProcessing] = useState(false);
  const [error, setError] = useState('');
  const [debugLog, setDebugLog] = useState([]);
  const [selectedCompany, setSelectedCompany] = useState(null);
  const [showCsvModal, setShowCsvModal] = useState(false);
  const [csvContent, setCsvContent] = useState('');
  const [concurrency, setConcurrency] = useState(2);
  const [activeView, setActiveView] = useState('discover_accounts');
  const [trainingExamples, setTrainingExamples] = useState([]);
  const [editingCompany, setEditingCompany] = useState(null); // domain of company being edited
  const [editScores, setEditScores] = useState({});
  const fileInputRef = useRef(null);
  const stopRef = useRef(false);
  const autoStartRef = useRef(false);
  const [dbReady, setDbReady] = useState(false);
  
  // Persistent contacts (loaded from DB)
  const [allContacts, setAllContacts] = useState([]);
  const [selectedContactId, setSelectedContactId] = useState(null);
  
  // Discovery state
  const [discoverResults, setDiscoverResults] = useState([]);
  const [discoverLoading, setDiscoverLoading] = useState(false);
  const [discoverError, setDiscoverError] = useState('');
  const [discoverCursor, setDiscoverCursor] = useState(null);
  const [discoverPage, setDiscoverPage] = useState(1);
  const [discoverTotal, setDiscoverTotal] = useState(0);
  const [discoverSelected, setDiscoverSelected] = useState(new Set());
  const [discoverMode, setDiscoverMode] = useState('indb'); // 'indb' | 'linkedin'
  const [showFilterPicker, setShowFilterPicker] = useState(false);
  const [filterPickerSearch, setFilterPickerSearch] = useState('');
  const filterPickerRef = useRef(null);
  const [discoverSort, setDiscoverSort] = useState({ key: 'employees', dir: 'desc' });
  
  // API-level sort — determines which companies Crustdata returns
  const DISCOVER_API_SORTS = [
    { key: 'last_funding_date_desc', label: 'Recently Funded', column: 'last_funding_date', order: 'desc' },
    { key: 'employee_desc', label: 'Most Employees', column: 'employee_metrics.latest_count', order: 'desc' },
    { key: 'employee_asc', label: 'Fewest Employees', column: 'employee_metrics.latest_count', order: 'asc' },
    { key: 'growth_6m_desc', label: 'Fastest HC Growth (6m)', column: 'employee_metrics.growth_6m_percent', order: 'desc' },
    { key: 'growth_12m_desc', label: 'Fastest HC Growth (12m)', column: 'employee_metrics.growth_12m_percent', order: 'desc' },
    { key: 'founded_desc', label: 'Newest Founded', column: 'year_founded', order: 'desc' },
    { key: 'founded_asc', label: 'Oldest Founded', column: 'year_founded', order: 'asc' },
    { key: 'followers_desc', label: 'Most Followers', column: 'follower_metrics.latest_count', order: 'desc' },
    { key: 'follower_growth_desc', label: 'Fastest Follower Growth', column: 'follower_metrics.growth_6m_percent', order: 'desc' },
    { key: 'funding_desc', label: 'Most Total Funding', column: 'crunchbase_total_investment_usd', order: 'desc' },
    { key: 'visitors_desc', label: 'Most Web Traffic', column: 'monthly_visitors', order: 'desc' },
    { key: 'jobs_desc', label: 'Most Job Openings', column: 'job_openings_count', order: 'desc' },
    { key: 'none', label: 'No Sort (API default)', column: null, order: null },
  ];
  const [discoverApiSort, setDiscoverApiSort] = useState('last_funding_date_desc');
  const [showColumnPicker, setShowColumnPicker] = useState(false);
  const columnPickerRef = useRef(null);

  // All available columns for discover results
  const ALL_DISCOVER_COLUMNS = [
    { key: 'name', label: 'Company', width: 2, type: 'text' },
    { key: 'domain', label: 'Domain', width: 1, type: 'link' },
    { key: 'employees', label: 'Employees', width: 1, type: 'number', align: 'right' },
    { key: 'investors', label: 'Investors', width: 2, type: 'text' },
    { key: 'funding', label: 'Funding Stage', width: 1, type: 'text' },
    { key: 'lastFundingDate', label: 'Last Funded', width: 1, type: 'date' },
    { key: 'totalFunding', label: 'Total Funding ($)', width: 1, type: 'money', align: 'right' },
    { key: 'categories', label: 'Categories', width: 2, type: 'text' },
    { key: 'markets', label: 'Markets', width: 2, type: 'text' },
    { key: 'description', label: 'Description', width: 2, type: 'text' },
    { key: 'industry', label: 'Industry', width: 2, type: 'text' },
    { key: 'location', label: 'HQ Location', width: 1, type: 'text' },
    { key: 'hqCountry', label: 'HQ Country', width: 1, type: 'text' },
    { key: 'largestHcCountry', label: 'Largest HC Country', width: 1, type: 'text' },
    { key: 'yearFounded', label: 'Year Founded', width: 1, type: 'number', align: 'right' },
    { key: 'employeeRange', label: 'Employee Range', width: 1, type: 'text' },
    { key: 'hcEngineering', label: 'Eng. HC', width: 1, type: 'number', align: 'right' },
    { key: 'hcSales', label: 'Sales HC', width: 1, type: 'number', align: 'right' },
    { key: 'hcMarketing', label: 'Marketing HC', width: 1, type: 'number', align: 'right' },
    { key: 'hcOperations', label: 'Ops HC', width: 1, type: 'number', align: 'right' },
    { key: 'hcHR', label: 'HR HC', width: 1, type: 'number', align: 'right' },
    { key: 'growth6m', label: 'HC Growth 6m %', width: 1, type: 'percent', align: 'right' },
    { key: 'growth12m', label: 'HC Growth 12m %', width: 1, type: 'percent', align: 'right' },
    { key: 'growth12mAbs', label: 'HC Growth 12m (abs)', width: 1, type: 'number', align: 'right' },
    { key: 'engGrowth6m', label: 'Eng. Growth 6m %', width: 1, type: 'percent', align: 'right' },
    { key: 'salesGrowth6m', label: 'Sales Growth 6m %', width: 1, type: 'percent', align: 'right' },
    { key: 'mktgGrowth6m', label: 'Marketing Growth 6m %', width: 1, type: 'percent', align: 'right' },
    { key: 'followers', label: 'LinkedIn Followers', width: 1, type: 'number', align: 'right' },
    { key: 'followerGrowth6m', label: 'Follower Growth 6m %', width: 1, type: 'percent', align: 'right' },
    { key: 'monthlyVisitors', label: 'Monthly Visitors', width: 1, type: 'number', align: 'right' },
    { key: 'jobOpenings', label: 'Job Openings', width: 1, type: 'number', align: 'right' },
    { key: 'overallRating', label: 'Glassdoor Rating', width: 1, type: 'number', align: 'right' },
    { key: 'revenueRange', label: 'Est. Revenue', width: 1, type: 'text', align: 'right' },
    { key: 'acquisitionStatus', label: 'Acq. Status', width: 1, type: 'text' },
    { key: 'companyType', label: 'Company Type', width: 1, type: 'text' },
    { key: 'ipoDate', label: 'IPO Date', width: 1, type: 'date' },
  ];

  const DEFAULT_COLUMNS = ['name', 'domain', 'employees', 'investors', 'funding', 'categories', 'description'];
  const [discoverColumns, setDiscoverColumns] = useState(DEFAULT_COLUMNS);

  // Saved filter presets
  const [savedFilters, setSavedFilters] = useState([]);
  const [showSaveDialog, setShowSaveDialog] = useState(false);
  const [saveFilterName, setSaveFilterName] = useState('');
  const [showSavedList, setShowSavedList] = useState(false);
  const savedListRef = useRef(null);

  // Dynamic filter system — each active filter is { id, fieldKey, operator, value }
  const [activeFilters, setActiveFilters] = useState([]);
  const filterIdCounter = useRef(0);

  // Autocomplete state for text/multi-value filters
  const [autocompleteResults, setAutocompleteResults] = useState({});
  const [autocompleteLoading, setAutocompleteLoading] = useState({});
  const autocompleteTimers = useRef({});

  // Crustdata CompanyDB filter catalog — all available filters from the API
  const FILTER_CATALOG = [
    // Company Identity
    { key: 'company_name', label: 'Company Name', category: 'Identity', inputType: 'autocomplete_text', operators: ['(.)', '[.]', '='], defaultOp: '(.)', autocompleteField: 'company_name' },
    { key: 'company_website_domain', label: 'Website Domain', category: 'Identity', inputType: 'autocomplete_text', operators: ['=', '(.)', 'in', 'not_in'], defaultOp: '(.)', autocompleteField: 'company_website_domain' },
    { key: 'company_type', label: 'Company Type', category: 'Identity', inputType: 'autocomplete_text', operators: ['=', '!='], defaultOp: '=', autocompleteField: 'company_type' },
    { key: 'linkedin_profile_url', label: 'LinkedIn URL', category: 'Identity', inputType: 'text', operators: ['='], defaultOp: '=' },
    { key: 'linkedin_id', label: 'LinkedIn ID', category: 'Identity', inputType: 'text', operators: ['=', 'in'], defaultOp: '=' },
    { key: 'year_founded', label: 'Year Founded', category: 'Identity', inputType: 'number', operators: ['=', '>', '<', '=>', '=<'], defaultOp: '=>' },
    { key: 'acquisition_status', label: 'Acquisition Status', category: 'Identity', inputType: 'autocomplete_text', operators: ['=', '!='], defaultOp: '=', autocompleteField: 'acquisition_status' },
    // Industry & Categories
    { key: 'linkedin_industries', label: 'LinkedIn Industries', category: 'Industry', inputType: 'autocomplete_multi', operators: ['in', 'not_in', '(.)'], defaultOp: 'in', autocompleteField: 'linkedin_industries' },
    { key: 'crunchbase_categories', label: 'Crunchbase Categories', category: 'Industry', inputType: 'autocomplete_multi', operators: ['in', 'not_in', '(.)'], defaultOp: 'in', autocompleteField: 'crunchbase_categories' },
    { key: 'markets', label: 'Markets', category: 'Industry', inputType: 'autocomplete_multi', operators: ['in', 'not_in', '(.)'], defaultOp: 'in', autocompleteField: 'markets' },
    // Size & Growth
    { key: 'employee_metrics.latest_count', label: 'Employee Count', category: 'Size', inputType: 'number', operators: ['=>', '=<', '>', '<', '='], defaultOp: '=>' },
    { key: 'employee_count_range', label: 'Employee Range', category: 'Size', inputType: 'multi_select', operators: ['in'], defaultOp: 'in', options: ['1-10', '11-50', '51-200', '201-500', '501-1000', '1001-5000', '5001-10000', '10001+'] },
    { key: 'employee_metrics.growth_6m_percent', label: 'Growth 6m %', category: 'Size', inputType: 'number', operators: ['>', '<', '=>', '=<'], defaultOp: '>' },
    { key: 'employee_metrics.growth_12m_percent', label: 'Growth 12m %', category: 'Size', inputType: 'number', operators: ['>', '<', '=>', '=<'], defaultOp: '>' },
    { key: 'employee_metrics.growth_12m', label: 'Growth 12m (abs)', category: 'Size', inputType: 'number', operators: ['>', '<', '=>', '=<'], defaultOp: '>' },
    // Department Headcount
    { key: 'department_metrics.engineering.latest_count', label: 'Engineering Headcount', category: 'Departments', inputType: 'number', operators: ['=>', '=<', '>', '<'], defaultOp: '=>' },
    { key: 'department_metrics.sales.latest_count', label: 'Sales Headcount', category: 'Departments', inputType: 'number', operators: ['=>', '=<', '>', '<'], defaultOp: '=>' },
    { key: 'department_metrics.marketing.latest_count', label: 'Marketing Headcount', category: 'Departments', inputType: 'number', operators: ['=>', '=<', '>', '<'], defaultOp: '=>' },
    { key: 'department_metrics.operations.latest_count', label: 'Operations Headcount', category: 'Departments', inputType: 'number', operators: ['=>', '=<', '>', '<'], defaultOp: '=>' },
    { key: 'department_metrics.human_resource.latest_count', label: 'HR Headcount', category: 'Departments', inputType: 'number', operators: ['=>', '=<', '>', '<'], defaultOp: '=>' },
    { key: 'department_metrics.engineering.growth_6m_percent', label: 'Engineering Growth 6m %', category: 'Departments', inputType: 'number', operators: ['>', '<', '=>', '=<'], defaultOp: '>' },
    { key: 'department_metrics.sales.growth_6m_percent', label: 'Sales Growth 6m %', category: 'Departments', inputType: 'number', operators: ['>', '<', '=>', '=<'], defaultOp: '>' },
    { key: 'department_metrics.marketing.growth_6m_percent', label: 'Marketing Growth 6m %', category: 'Departments', inputType: 'number', operators: ['>', '<', '=>', '=<'], defaultOp: '>' },
    // Funding
    { key: 'last_funding_round_type', label: 'Funding Stage', category: 'Funding', inputType: 'multi_select', operators: ['in', 'not_in'], defaultOp: 'in', options: ['seed', 'pre_seed', 'angel', 'series_a', 'series_b', 'series_c', 'series_d', 'series_e', 'series_f', 'series_g', 'series_h', 'debt_financing', 'convertible_note', 'grant', 'private_equity', 'secondary_market', 'undisclosed'] },
    { key: 'last_funding_date', label: 'Last Funding Date', category: 'Funding', inputType: 'date', operators: ['=>', '=<', '>', '<'], defaultOp: '=>' },
    { key: 'crunchbase_total_investment_usd', label: 'Total Funding ($)', category: 'Funding', inputType: 'number', operators: ['>', '<', '=>', '=<'], defaultOp: '>' },
    { key: 'crunchbase_investors', label: 'Investors', category: 'Funding', inputType: 'autocomplete_multi', operators: ['in'], defaultOp: 'in', autocompleteField: 'crunchbase_investors' },
    { key: 'tracxn_investors', label: 'Tracxn Investors', category: 'Funding', inputType: 'autocomplete_multi', operators: ['in'], defaultOp: 'in', autocompleteField: 'tracxn_investors' },
    // Location
    { key: 'hq_country', label: 'HQ Country', category: 'Location', inputType: 'autocomplete_multi', operators: ['in', 'not_in'], defaultOp: 'in', autocompleteField: 'hq_country' },
    { key: 'hq_location', label: 'HQ Location', category: 'Location', inputType: 'autocomplete_text', operators: ['(.)', '[.]'], defaultOp: '(.)', autocompleteField: 'hq_location' },
    { key: 'largest_headcount_country', label: 'Largest Headcount Country', category: 'Location', inputType: 'autocomplete_multi', operators: ['in', 'not_in'], defaultOp: 'in', autocompleteField: 'largest_headcount_country' },
    // Social
    { key: 'follower_metrics.latest_count', label: 'LinkedIn Followers', category: 'Social', inputType: 'number', operators: ['=>', '=<', '>', '<'], defaultOp: '>' },
    { key: 'follower_metrics.growth_6m_percent', label: 'Follower Growth 6m %', category: 'Social', inputType: 'number', operators: ['>', '<', '=>', '=<'], defaultOp: '>' },
    // Revenue
    { key: 'estimated_revenue_lower_bound_usd', label: 'Revenue Lower Bound ($)', category: 'Revenue', inputType: 'number', operators: ['=>', '>', '<', '=<'], defaultOp: '=>' },
    { key: 'estimated_revenue_higher_bound_usd', label: 'Revenue Upper Bound ($)', category: 'Revenue', inputType: 'number', operators: ['=<', '<', '>', '=>'], defaultOp: '=<' },
    // Competitors
    { key: 'competitor_websites', label: 'Competitor Websites', category: 'Advanced', inputType: 'text_list', operators: ['in'], defaultOp: 'in' },
    { key: 'competitor_ids', label: 'Competitor IDs', category: 'Advanced', inputType: 'text_list', operators: ['in'], defaultOp: 'in' },
    { key: 'ipo_date', label: 'IPO Date', category: 'Advanced', inputType: 'date', operators: ['=>', '=<', '>', '<'], defaultOp: '=>' },
  ];

  const OPERATOR_LABELS = {
    '=': 'equals', '!=': 'not equals', '>': '>', '<': '<', '=>': '≥', '=<': '≤',
    'in': 'in', 'not_in': 'not in', '(.)': 'contains', '[.]': 'exact match',
  };

  const addFilter = (fieldKey) => {
    const spec = FILTER_CATALOG.find(f => f.key === fieldKey);
    if (!spec) return;
    const id = ++filterIdCounter.current;
    const defaultValue = spec.inputType === 'multi_select' || spec.inputType === 'autocomplete_multi' || spec.inputType === 'text_list' ? [] : '';
    setActiveFilters(prev => [...prev, { id, fieldKey, operator: spec.defaultOp, value: defaultValue }]);
    setShowFilterPicker(false);
    setFilterPickerSearch('');
  };

  const updateFilter = (id, updates) => {
    setActiveFilters(prev => prev.map(f => f.id === id ? { ...f, ...updates } : f));
  };

  const removeFilter = (id) => {
    setActiveFilters(prev => prev.filter(f => f.id !== id));
  };

  // Autocomplete for filter fields
  const fetchAutocomplete = async (filterId, field, query) => {
    if (autocompleteTimers.current[filterId]) clearTimeout(autocompleteTimers.current[filterId]);
    if (!query || query.length < 1) { setAutocompleteResults(prev => ({ ...prev, [filterId]: [] })); return; }
    autocompleteTimers.current[filterId] = setTimeout(async () => {
      setAutocompleteLoading(prev => ({ ...prev, [filterId]: true }));
      try {
        const resp = await fetch('/api/crustdata', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ action: 'autocomplete', field, query, limit: 10 }),
        });
        const data = await resp.json();
        setAutocompleteResults(prev => ({ ...prev, [filterId]: data.values || data.suggestions || [] }));
      } catch { setAutocompleteResults(prev => ({ ...prev, [filterId]: [] })); }
      setAutocompleteLoading(prev => ({ ...prev, [filterId]: false }));
    }, 250);
  };

  // Close filter picker on click outside
  useEffect(() => {
    const handler = (e) => {
      if (filterPickerRef.current && !filterPickerRef.current.contains(e.target)) setShowFilterPicker(false);
      if (savedListRef.current && !savedListRef.current.contains(e.target)) setShowSavedList(false);
      if (columnPickerRef.current && !columnPickerRef.current.contains(e.target)) setShowColumnPicker(false);
      if (peopleFilterPickerRef.current && !peopleFilterPickerRef.current.contains(e.target)) setShowPeopleFilterPicker(false);
      if (contactColumnPickerRef.current && !contactColumnPickerRef.current.contains(e.target)) setShowContactColumnPicker(false);
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  // Load saved filters on mount
  useEffect(() => {
    if (supabase) { getSavedFilters().then(setSavedFilters).catch(() => {}); }
  }, []);
  
  // Load training examples on mount
  useEffect(() => {
    if (supabase) { getTrainingExamples().then(setTrainingExamples).catch(() => {}); }
  }, []);

  const handleSaveFilters = async () => {
    if (!saveFilterName.trim()) return;
    const filtersData = discoverMode === 'indb' ? activeFilters : linkedinFilters;
    if (filtersData.length === 0) return;
    try {
      const saved = await createSavedFilter(saveFilterName.trim(), discoverMode, filtersData);
      if (saved) setSavedFilters(prev => [saved, ...prev]);
      setSaveFilterName('');
      setShowSaveDialog(false);
      addLog(`Saved filter preset: "${saveFilterName.trim()}"`);
    } catch (err) { addLog(`Save filter error: ${err.message}`); }
  };

  const handleLoadFilter = (preset) => {
    if (preset.mode !== discoverMode) setDiscoverMode(preset.mode);
    // Small timeout to let mode switch render before setting filters
    setTimeout(() => {
      if (preset.mode === 'indb') {
        // Re-assign IDs to avoid collisions
        const loaded = (preset.filters || []).map(f => ({ ...f, id: ++filterIdCounter.current }));
        setActiveFilters(loaded);
      } else {
        const loaded = (preset.filters || []).map(f => ({ ...f, id: ++linkedinFilterIdCounter.current }));
        setLinkedinFilters(loaded);
      }
    }, 50);
    setShowSavedList(false);
    setDiscoverResults([]);
    setDiscoverTotal(0);
    addLog(`Loaded filter preset: "${preset.name}" (${preset.mode})`);
  };

  const handleUpdateSavedFilter = async (preset) => {
    const filtersData = discoverMode === 'indb' ? activeFilters : linkedinFilters;
    if (filtersData.length === 0 || preset.mode !== discoverMode) return;
    try {
      const updated = await updateSavedFilter(preset.id, { filters: filtersData });
      if (updated) setSavedFilters(prev => prev.map(f => f.id === preset.id ? updated : f));
      addLog(`Updated filter preset: "${preset.name}"`);
    } catch (err) { addLog(`Update filter error: ${err.message}`); }
  };

  const handleDeleteSavedFilter = async (presetId) => {
    if (!confirm('Delete this saved filter?')) return;
    try {
      await deleteSavedFilter(presetId);
      setSavedFilters(prev => prev.filter(f => f.id !== presetId));
    } catch (err) { addLog(`Delete filter error: ${err.message}`); }
  };

  // ======= LINKEDIN COMPANY SEARCH FILTER SYSTEM =======
  const DEPARTMENTS = ['Accounting', 'Administrative', 'Arts and Design', 'Business Development', 'Community and Social Services', 'Consulting', 'Education', 'Engineering', 'Entrepreneurship', 'Finance', 'Healthcare Services', 'Human Resources', 'Information Technology', 'Legal', 'Marketing', 'Media and Communication', 'Military and Protective Services', 'Operations', 'Product Management', 'Program and Project Management', 'Purchasing', 'Quality Assurance', 'Real Estate', 'Research', 'Sales', 'Customer Success and Support'];

  const LINKEDIN_FILTER_CATALOG = [
    { key: 'COMPANY_HEADCOUNT', label: 'Company Headcount', category: 'Size', filterKind: 'text',
      options: ['1-10', '11-50', '51-200', '201-500', '501-1,000', '1,001-5,000', '5,001-10,000', '10,001+'] },
    { key: 'COMPANY_HEADCOUNT_GROWTH', label: 'Headcount Growth %', category: 'Size', filterKind: 'range', noSubFilter: true },
    { key: 'REGION', label: 'Region', category: 'Location', filterKind: 'text', supportsNotIn: true, usesAutocomplete: 'REGION' },
    { key: 'INDUSTRY', label: 'Industry', category: 'Industry', filterKind: 'text', supportsNotIn: true, usesAutocomplete: 'INDUSTRY' },
    { key: 'KEYWORD', label: 'Keyword', category: 'Search', filterKind: 'keyword' },
    { key: 'NUM_OF_FOLLOWERS', label: 'Followers', category: 'Social', filterKind: 'text',
      options: ['1-50', '51-100', '101-1000', '1001-5000', '5001+'] },
    { key: 'FORTUNE', label: 'Fortune Ranking', category: 'Signals', filterKind: 'text',
      options: ['Fortune 50', 'Fortune 51-100', 'Fortune 101-250', 'Fortune 251-500'] },
    { key: 'ACCOUNT_ACTIVITIES', label: 'Account Activities', category: 'Signals', filterKind: 'text',
      options: ['Senior leadership changes in last 3 months', 'Funding events in past 12 months'] },
    { key: 'JOB_OPPORTUNITIES', label: 'Job Opportunities', category: 'Signals', filterKind: 'text',
      options: ['Hiring on Linkedin'] },
    { key: 'IN_THE_NEWS', label: 'In the News', category: 'Signals', filterKind: 'boolean' },
    { key: 'ANNUAL_REVENUE', label: 'Annual Revenue ($M)', category: 'Financials', filterKind: 'range', subFilterOptions: ['USD', 'EUR', 'GBP', 'CAD', 'AUD', 'JPY', 'INR', 'CNY', 'BRL', 'SEK', 'NOK', 'DKK', 'SGD', 'HKD', 'NZD', 'ILS', 'IDR', 'THB', 'TRY', 'TWD', 'AED', 'RUB'], defaultSubFilter: 'USD' },
    { key: 'DEPARTMENT_HEADCOUNT', label: 'Department Headcount', category: 'Departments', filterKind: 'range', subFilterOptions: DEPARTMENTS },
    { key: 'DEPARTMENT_HEADCOUNT_GROWTH', label: 'Department Growth %', category: 'Departments', filterKind: 'range', subFilterOptions: DEPARTMENTS },
  ];

  const [linkedinFilters, setLinkedinFilters] = useState([]);
  const linkedinFilterIdCounter = useRef(0);

  const addLinkedinFilter = (filterKey) => {
    const spec = LINKEDIN_FILTER_CATALOG.find(f => f.key === filterKey);
    if (!spec) return;
    const id = ++linkedinFilterIdCounter.current;
    let initial;
    if (spec.filterKind === 'text') initial = { id, filterKey, type: 'in', value: [], notIn: false };
    else if (spec.filterKind === 'range') initial = { id, filterKey, type: 'between', min: '', max: '', subFilter: spec.defaultSubFilter || (spec.subFilterOptions ? spec.subFilterOptions[0] : '') };
    else if (spec.filterKind === 'boolean') initial = { id, filterKey };
    else if (spec.filterKind === 'keyword') initial = { id, filterKey, type: 'in', value: '' };
    setLinkedinFilters(prev => [...prev, initial]);
    setShowFilterPicker(false);
    setFilterPickerSearch('');
  };

  const updateLinkedinFilter = (id, updates) => {
    setLinkedinFilters(prev => prev.map(f => f.id === id ? { ...f, ...updates } : f));
  };

  const removeLinkedinFilter = (id) => {
    setLinkedinFilters(prev => prev.filter(f => f.id !== id));
  };

  // Autocomplete for LinkedIn filters (REGION, INDUSTRY)
  const fetchLinkedinAutocomplete = async (filterId, filterType, query) => {
    if (autocompleteTimers.current[`li_${filterId}`]) clearTimeout(autocompleteTimers.current[`li_${filterId}`]);
    if (!query || query.length < 1) { setAutocompleteResults(prev => ({ ...prev, [`li_${filterId}`]: [] })); return; }
    autocompleteTimers.current[`li_${filterId}`] = setTimeout(async () => {
      setAutocompleteLoading(prev => ({ ...prev, [`li_${filterId}`]: true }));
      try {
        const resp = await fetch('/api/crustdata', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ action: 'filters_autocomplete', filter_type: filterType, query, limit: 10 }),
        });
        const data = await resp.json();
        // Filters autocomplete returns array of objects with 'value' key, or plain array
        const values = Array.isArray(data) ? data.map(d => typeof d === 'string' ? d : d.value || d.name || '') : (data.values || data.suggestions || []);
        setAutocompleteResults(prev => ({ ...prev, [`li_${filterId}`]: values.filter(Boolean) }));
      } catch { setAutocompleteResults(prev => ({ ...prev, [`li_${filterId}`]: [] })); }
      setAutocompleteLoading(prev => ({ ...prev, [`li_${filterId}`]: false }));
    }, 250);
  };

  const buildLinkedInFilters = () => {
    const filters = [];
    for (const f of linkedinFilters) {
      const spec = LINKEDIN_FILTER_CATALOG.find(s => s.key === f.filterKey);
      if (!spec) continue;

      if (spec.filterKind === 'boolean') {
        filters.push({ filter_type: f.filterKey });
      } else if (spec.filterKind === 'keyword') {
        if (f.value && f.value.trim()) filters.push({ filter_type: 'KEYWORD', type: 'in', value: [f.value.trim()] });
      } else if (spec.filterKind === 'text') {
        if (Array.isArray(f.value) && f.value.length > 0) {
          filters.push({ filter_type: f.filterKey, type: f.notIn ? 'not in' : 'in', value: f.value });
        }
      } else if (spec.filterKind === 'range') {
        const min = f.min !== '' ? Number(f.min) : null;
        const max = f.max !== '' ? Number(f.max) : null;
        if (min !== null || max !== null) {
          const filter = { filter_type: f.filterKey, type: 'between', value: {} };
          if (min !== null) filter.value.min = min;
          if (max !== null) filter.value.max = max;
          if (f.subFilter) filter.sub_filter = f.subFilter;
          filters.push(filter);
        }
      }
    }
    return filters;
  };
  
  // Contacts search state
  const [contactsCompany, setContactsCompany] = useState(null); // null = all screened companies, or {domain, name, linkedin}
  const [contactsResults, setContactsResults] = useState([]);
  const [contactsLoading, setContactsLoading] = useState(false);
  const [contactsError, setContactsError] = useState('');
  const [contactsCursor, setContactsCursor] = useState(null);
  const [contactsTotal, setContactsTotal] = useState(0);
  const [selectedContactResults, setSelectedContactResults] = useState(new Set());
  const [contactsFilters, setContactsFilters] = useState({
    titles: '',
    functions: [],
    verifiedEmailOnly: false,
    recentlyChangedJobs: false,
  });

  // Contact results column system
  const ALL_CONTACT_COLUMNS = [
    { key: 'name', label: 'Name', type: 'text' },
    { key: 'title', label: 'Title', type: 'text' },
    { key: 'company', label: 'Company', type: 'text' },
    { key: 'seniority', label: 'Seniority', type: 'text' },
    { key: 'function', label: 'Function', type: 'text' },
    { key: 'region', label: 'Region', type: 'text' },
    { key: 'headline', label: 'Headline', type: 'text' },
    { key: 'experience', label: 'Years Exp.', type: 'number' },
    { key: 'connections', label: 'Connections', type: 'number' },
    { key: 'jobStartDate', label: 'Current Role Since', type: 'date' },
    { key: 'jobLocation', label: 'Job Location', type: 'text' },
    { key: 'companyDomain', label: 'Company Domain', type: 'text' },
    { key: 'skills', label: 'Skills', type: 'text' },
    { key: 'education', label: 'Education', type: 'text' },
    { key: 'allTitles', label: 'All Titles', type: 'text' },
    { key: 'summary', label: 'Summary', type: 'text' },
  ];
  const DEFAULT_CONTACT_COLUMNS = ['name', 'title', 'company', 'seniority', 'function', 'region'];
  const [contactResultColumns, setContactResultColumns] = useState(DEFAULT_CONTACT_COLUMNS);
  const [showContactColumnPicker, setShowContactColumnPicker] = useState(false);
  const contactColumnPickerRef = useRef(null);
  const [titleSuggestions, setTitleSuggestions] = useState([]);
  const [titleAutocompleteLoading, setTitleAutocompleteLoading] = useState(false);
  
  const FUNCTION_OPTIONS = ['Marketing', 'Sales', 'Engineering', 'Product Management', 'Operations', 'Finance', 'Human Resources', 'Design', 'Customer Success', 'Business Development'];

  // === PEOPLE FILTER CATALOG (mirrors company FILTER_CATALOG pattern) ===
  const PEOPLE_FILTER_CATALOG = [
    // Job
    { key: 'current_employers.title', label: 'Job Title', category: 'Job', inputType: 'people_autocomplete_text', operators: ['(.)', '[.]', '='], defaultOp: '(.)', autocompleteField: 'current_employers.title' },
    { key: 'current_employers.function_category', label: 'Function', category: 'Job', inputType: 'multi_select', operators: ['in', 'not_in'], defaultOp: 'in', options: ['Accounting', 'Administrative', 'Arts and Design', 'Business Development', 'Community and Social Services', 'Consulting', 'Education', 'Engineering', 'Entrepreneurship', 'Finance', 'Healthcare Services', 'Human Resources', 'Information Technology', 'Legal', 'Marketing', 'Media and Communication', 'Military and Protective Services', 'Operations', 'Product Management', 'Program and Project Management', 'Purchasing', 'Quality Assurance', 'Real Estate', 'Research', 'Sales', 'Customer Success and Support'] },
    { key: 'current_employers.seniority_level', label: 'Seniority', category: 'Job', inputType: 'multi_select', operators: ['in', 'not_in'], defaultOp: 'in', options: ['CXO', 'VP', 'Director', 'Manager', 'Senior', 'Entry', 'Intern'] },
    { key: 'current_employers.department', label: 'Department', category: 'Job', inputType: 'people_autocomplete_text', operators: ['(.)', '=', 'in'], defaultOp: '(.)', autocompleteField: 'current_employers.department' },
    // Company
    { key: 'current_employers.company_name', label: 'Company Name', category: 'Company', inputType: 'people_autocomplete_text', operators: ['(.)', '='], defaultOp: '(.)', autocompleteField: 'current_employers.company_name' },
    { key: 'current_employers.company_website_domain', label: 'Company Domain', category: 'Company', inputType: 'text', operators: ['=', 'in'], defaultOp: '=' },
    { key: 'current_employers.company_linkedin_url', label: 'Company LinkedIn', category: 'Company', inputType: 'text', operators: ['='], defaultOp: '=' },
    // Person
    { key: 'name', label: 'Name', category: 'Person', inputType: 'text', operators: ['(.)', '='], defaultOp: '(.)'  },
    { key: 'region', label: 'Region', category: 'Person', inputType: 'people_autocomplete_text', operators: ['(.)', '=', 'in'], defaultOp: '(.)', autocompleteField: 'region' },
    { key: 'country', label: 'Country', category: 'Person', inputType: 'people_autocomplete_text', operators: ['=', 'in'], defaultOp: '=', autocompleteField: 'country' },
    { key: 'years_of_experience_raw', label: 'Years Experience', category: 'Person', inputType: 'number', operators: ['=>', '=<', '>', '<', '='], defaultOp: '=>' },
    // Status
    { key: 'recently_changed_jobs', label: 'Recently Changed Jobs', category: 'Status', inputType: 'boolean', operators: ['='], defaultOp: '=', defaultValue: true },
    { key: 'current_employers.business_email_verified', label: 'Verified Email', category: 'Status', inputType: 'boolean', operators: ['='], defaultOp: '=', defaultValue: true },
  ];

  const [peopleFilters, setPeopleFilters] = useState([]);
  const peopleFilterIdCounter = useRef(0);
  const [showPeopleFilterPicker, setShowPeopleFilterPicker] = useState(false);
  const peopleFilterPickerRef = useRef(null);
  const [peopleAutocompleteResults, setPeopleAutocompleteResults] = useState({});
  const [peopleAutocompleteLoading, setPeopleAutocompleteLoading] = useState({});
  const peopleAutocompleteTimers = useRef({});

  const addPeopleFilter = (fieldKey) => {
    const spec = PEOPLE_FILTER_CATALOG.find(f => f.key === fieldKey);
    if (!spec) return;
    const id = ++peopleFilterIdCounter.current;
    const defaultValue = spec.inputType === 'boolean' ? (spec.defaultValue !== undefined ? spec.defaultValue : true) :
                          spec.inputType === 'multi_select' ? [] : '';
    setPeopleFilters(prev => [...prev, { id, fieldKey, operator: spec.defaultOp, value: defaultValue }]);
    setShowPeopleFilterPicker(false);
  };

  const updatePeopleFilter = (id, updates) => {
    setPeopleFilters(prev => prev.map(f => f.id === id ? { ...f, ...updates } : f));
  };

  const removePeopleFilter = (id) => {
    setPeopleFilters(prev => prev.filter(f => f.id !== id));
    setPeopleAutocompleteResults(prev => { const n = { ...prev }; delete n[id]; return n; });
  };

  const fetchPeopleAutocomplete = async (filterId, field, query) => {
    if (peopleAutocompleteTimers.current[filterId]) clearTimeout(peopleAutocompleteTimers.current[filterId]);
    if (!query || query.length < 1) { setPeopleAutocompleteResults(prev => ({ ...prev, [filterId]: [] })); return; }
    peopleAutocompleteTimers.current[filterId] = setTimeout(async () => {
      setPeopleAutocompleteLoading(prev => ({ ...prev, [filterId]: true }));
      try {
        const resp = await fetch('/api/crustdata', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ action: 'people_autocomplete', field, query, limit: 10 }),
        });
        const data = await resp.json();
        const results = Array.isArray(data) ? data.map(d => typeof d === 'string' ? d : d.value || d.label || '').filter(Boolean) :
                        (data.values || data.suggestions || []);
        setPeopleAutocompleteResults(prev => ({ ...prev, [filterId]: results }));
      } catch { setPeopleAutocompleteResults(prev => ({ ...prev, [filterId]: [] })); }
      setPeopleAutocompleteLoading(prev => ({ ...prev, [filterId]: false }));
    }, 250);
  };

  // Build filters from PEOPLE_FILTER_CATALOG dynamic filters
  const buildDynamicPeopleFilters = () => {
    const conditions = [];

    // Always add company domain filter
    if (!contactsCompany) return null;
    if (Array.isArray(contactsCompany)) {
      const domains = contactsCompany.map(c => c.domain).filter(Boolean);
      if (domains.length === 0) return null;
      if (domains.length === 1) {
        conditions.push({ column: 'current_employers.company_website_domain', type: '=', value: domains[0] });
      } else {
        conditions.push({ column: 'current_employers.company_website_domain', type: 'in', value: domains });
      }
    } else {
      if (!contactsCompany.domain) return null;
      conditions.push({ column: 'current_employers.company_website_domain', type: '=', value: contactsCompany.domain });
    }

    // Add dynamic filters
    for (const f of peopleFilters) {
      const spec = PEOPLE_FILTER_CATALOG.find(s => s.key === f.fieldKey);
      if (!spec) continue;

      if (spec.inputType === 'boolean') {
        conditions.push({ column: f.fieldKey, type: '=', value: f.value });
      } else if (spec.inputType === 'multi_select') {
        if (Array.isArray(f.value) && f.value.length > 0) {
          conditions.push({ column: f.fieldKey, type: f.operator, value: f.value });
        }
      } else if (spec.inputType === 'number') {
        if (f.value !== '' && f.value !== undefined) {
          conditions.push({ column: f.fieldKey, type: f.operator, value: parseFloat(f.value) });
        }
      } else {
        // text, autocomplete
        if (typeof f.value === 'string' && f.value.trim()) {
          // Handle comma-separated values for text fields with fuzzy
          if (f.operator === '(.)' && f.value.includes(',')) {
            const vals = f.value.split(',').map(v => v.trim()).filter(Boolean);
            if (vals.length === 1) {
              conditions.push({ column: f.fieldKey, type: '(.)', value: vals[0] });
            } else {
              conditions.push({ op: 'or', conditions: vals.map(v => ({ column: f.fieldKey, type: '(.)', value: v })) });
            }
          } else if (f.operator === 'in' && f.value.includes(',')) {
            const vals = f.value.split(',').map(v => v.trim()).filter(Boolean);
            conditions.push({ column: f.fieldKey, type: 'in', value: vals });
          } else {
            conditions.push({ column: f.fieldKey, type: f.operator, value: f.value.trim() });
          }
        }
      }
    }

    if (conditions.length === 0) return null;
    return { op: 'and', conditions };
  };

  const searchContactsDynamic = async () => {
    setContactsLoading(true);
    setContactsError('');
    setContactsResults([]);
    setSelectedContactResults(new Set());
    setContactsCursor(null);
    setContactsTotal(0);

    try {
      const filters = buildDynamicPeopleFilters();
      if (!filters) { setContactsError('No filters configured.'); setContactsLoading(false); return; }

      addLog(`Contact search (dynamic): ${JSON.stringify(filters).substring(0, 500)}`);

      const resp = await fetch('/api/crustdata', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: 'people_search', filters, limit: 50 }),
      });
      const data = await resp.json();
      if (data.error) throw new Error(data.error);

      const people = (data.profiles || []).map(parsePersonProfile);

      setContactsResults(people);
      setContactsTotal(data.total_count || people.length);
      setSelectedContactResults(new Set(people.map(p => p.id || p.linkedin)));
      setContactsCursor(data.next_cursor || null);
      addLog(`Contact search: ${people.length} results (${data.total_count || 0} total)`);
    } catch (err) {
      setContactsError(err.message);
      addLog(`Contact search error: ${err.message}`);
    }
    setContactsLoading(false);
  };
  const [enrichingContacts, setEnrichingContacts] = useState({}); // { contactId: 'loading' | 'done' | 'error' }

  // Campaign state
  const [campaigns, setCampaigns] = useState([]);
  const [activeCampaignId, setActiveCampaignId] = useState(null);
  const [campaignContacts, setCampaignContacts] = useState([]);
  const [allCampaignContacts, setAllCampaignContacts] = useState([]); // all campaign_contacts for filtering
  const [campaignMessages, setCampaignMessages] = useState([]);
  const [selectedCampaignContact, setSelectedCampaignContact] = useState(null);
  // Resizable campaign panels (widths in px)
  const [campPanelWidths, setCampPanelWidths] = useState([240, 340, null]); // left, middle, right=flex
  const campDragRef = useRef(null);
  // Prompt-driven generation
  const [campaignPrompt, setCampaignPrompt] = useState('');
  const [promptExpanded, setPromptExpanded] = useState(true);
  const [contactGenMessages, setContactGenMessages] = useState([]); // generated messages for selected contact
  const [contactDraftMessages, setContactDraftMessages] = useState([]); // pass 1 draft
  const [contactReviewNotes, setContactReviewNotes] = useState(''); // pass 2 findings
  const [contactGapSelection, setContactGapSelection] = useState(null); // gap selection reasoning
  const [generating, setGenerating] = useState(false);
  const [email1FactorOverride, setEmail1FactorOverride] = useState(''); // empty = use account's gap1Factor
  const [email2FactorOverride, setEmail2FactorOverride] = useState(''); // empty = use account's gap2Factor
  const [lastPromptInputs, setLastPromptInputs] = useState({ email1: null, email2: null }); // stores inputs passed to Sonnet
  
  // Training examples for few-shot email generation
  const [showManualEmailEditor, setShowManualEmailEditor] = useState(false);
  const [manualEmail, setManualEmail] = useState({ gap_factor: '', gap_name: '', opening: '', context: '', avoid_notes: '' });

  // === ALL PROMPTS: Supabase-backed with defaults from constants ===
  // Prompt keys use prefixes: scoring.system, scoring.A, research, obs.A, template.credibility_adg, etc.
  const [promptsLoaded, setPromptsLoaded] = useState(false);

  // State for each prompt group (initialized from defaults, overridden by Supabase on load)
  const [scoringPrompts, setScoringPrompts] = useState({
    system: SYSTEM_PROMPT_SCORER, disqualification: DISQUALIFICATION_PROMPT,
    A: FACTOR_A_PROMPT, B: FACTOR_B_PROMPT, C: FACTOR_C_PROMPT, D: FACTOR_D_PROMPT,
    E: FACTOR_E_PROMPT, F: FACTOR_F_PROMPT, G: FACTOR_G_PROMPT,
  });
  const [researchPrompt, setResearchPrompt] = useState(RESEARCH_PROMPT);
  const [obsPrompts, setObsPrompts] = useState({ ...DEFAULT_OBS_PROMPTS });
  const [emailTemplates, setEmailTemplates] = useState({ ...DEFAULT_EMAIL_TEMPLATES });

  // Load all prompts from Supabase on mount
  useEffect(() => {
    const loadPrompts = async () => {
      try {
        const saved = await getAllPrompts();
        if (Object.keys(saved).length > 0) {
          // Scoring prompts
          setScoringPrompts(prev => {
            const next = { ...prev };
            for (const key of ['system','disqualification','A','B','C','D','E','F','G']) {
              if (saved[`scoring.${key}`] !== undefined) next[key] = saved[`scoring.${key}`];
            }
            return next;
          });
          // Research prompt
          if (saved['research'] !== undefined) setResearchPrompt(saved['research']);
          // Observation prompts
          setObsPrompts(prev => {
            const next = { ...prev };
            for (const key of ['A','B','C','D','E','F','G']) {
              if (saved[`obs.${key}`] !== undefined) next[key] = saved[`obs.${key}`];
            }
            return next;
          });
          // Email templates
          setEmailTemplates(prev => {
            const next = { ...prev };
            for (const key of ['credibility_adg','credibility_bc','cta','email2_closing','signoff']) {
              if (saved[`template.${key}`] !== undefined) next[key] = saved[`template.${key}`];
            }
            return next;
          });
          addLog(`Loaded ${Object.keys(saved).length} prompts from database`);
        }
      } catch (err) {
        addLog(`Prompt load error: ${err.message}`);
      }
      setPromptsLoaded(true);
    };
    loadPrompts();
  }, []);

  // Debounced save to Supabase
  const promptSaveTimers = useRef({});
  const savePromptToDb = (promptKey, promptText) => {
    if (promptSaveTimers.current[promptKey]) clearTimeout(promptSaveTimers.current[promptKey]);
    promptSaveTimers.current[promptKey] = setTimeout(async () => {
      try { await upsertPrompt(promptKey, promptText); } catch (err) { console.log(`Prompt save error: ${err.message}`); }
    }, 1000); // 1 second debounce
  };

  // Update functions — update state immediately, save to Supabase with debounce
  const updateScoringPrompt = (key, value) => {
    setScoringPrompts(prev => ({ ...prev, [key]: value }));
    savePromptToDb(`scoring.${key}`, value);
  };
  const resetScoringPrompt = (key) => {
    const defaults = { system: SYSTEM_PROMPT_SCORER, disqualification: DISQUALIFICATION_PROMPT, A: FACTOR_A_PROMPT, B: FACTOR_B_PROMPT, C: FACTOR_C_PROMPT, D: FACTOR_D_PROMPT, E: FACTOR_E_PROMPT, F: FACTOR_F_PROMPT, G: FACTOR_G_PROMPT };
    updateScoringPrompt(key, defaults[key]);
  };

  const updateResearchPrompt = (value) => {
    setResearchPrompt(value);
    savePromptToDb('research', value);
  };
  const resetResearchPrompt = () => updateResearchPrompt(RESEARCH_PROMPT);

  const updateObsPrompt = (key, value) => {
    setObsPrompts(prev => ({ ...prev, [key]: value }));
    savePromptToDb(`obs.${key}`, value);
  };
  const resetObsPrompt = (key) => updateObsPrompt(key, DEFAULT_OBS_PROMPTS[key]);

  const updateEmailTemplate = (key, value) => {
    setEmailTemplates(prev => ({ ...prev, [key]: value }));
    savePromptToDb(`template.${key}`, value);
  };
  const resetEmailTemplate = (key) => updateEmailTemplate(key, DEFAULT_EMAIL_TEMPLATES[key]);
  const [genProgress, setGenProgress] = useState(''); // 'Generating 3/10...'
  const [genContactsWithMessages, setGenContactsWithMessages] = useState(new Set()); // contact IDs that have generated messages
  const [checkedContacts, setCheckedContacts] = useState(new Set()); // for contacts tab bulk actions
  const [checkedAccounts, setCheckedAccounts] = useState(new Set()); // for accounts tab bulk actions (domains)
  const [checkedStaged, setCheckedStaged] = useState(new Set()); // for screened accounts bulk actions
  const [selectedStagedCompany, setSelectedStagedCompany] = useState(null); // index into companies array for detail view
  const [hideAccountsWithContacts, setHideAccountsWithContacts] = useState(false);
  
  // Accounts CRM state
  const ACCOUNT_STATUSES = ['Cold', 'Engaged', 'Opportunity', 'Client'];
  const [accountSearch, setAccountSearch] = useState('');
  const [accountFitFilter, setAccountFitFilter] = useState([]); // array of fits: 'Strong', 'Moderate', 'Weak', 'Disqualified'
  const [accountStatusFilter, setAccountStatusFilter] = useState([]); // array of statuses: 'Cold', 'Engaged', etc.
  const [accountSort, setAccountSort] = useState('score'); // 'score' | 'name' | 'contacts' | 'status'
  const [accountAddedAfter, setAccountAddedAfter] = useState('');
  const [accountScreenedAfter, setAccountScreenedAfter] = useState('');

  // Contacts CRM state
  const CONTACT_STATUSES = ['New', 'Engaged', 'Opportunity', 'Client'];
  const [contactSearch, setContactSearch] = useState('');
  const [contactSeniorityFilter, setContactSeniorityFilter] = useState([]);
  const [contactFunctionFilter, setContactFunctionFilter] = useState([]);
  const [contactEmailFilter, setContactEmailFilter] = useState([]); // 'has_email', 'no_email', 'enriched'
  const [contactStatusFilter, setContactStatusFilter] = useState([]);
  const [contactCampaignFilter, setContactCampaignFilter] = useState([]);
  const [contactSort, setContactSort] = useState('newest');
  const [bulkStatusPicker, setBulkStatusPicker] = useState(false);

  // Instantly integration state
  const [instantlyCampaigns, setInstantlyCampaigns] = useState([]);
  const [instantlyPicker, setInstantlyPicker] = useState(false);
  const [instantlyLoading, setInstantlyLoading] = useState(false);
  const [instantlyResult, setInstantlyResult] = useState(null); // { type: 'success'|'error', message: string }

  // Industry autocomplete via Crustdata API
  // Load existing companies from Supabase on mount
  useEffect(() => {
    const loadFromDb = async () => {
      if (!supabase) { setDbReady(true); return; }
      try {
        const rows = await getCompaniesWithLatest();
        if (rows && rows.length > 0) {
          const loaded = rows.map(r => {
            // Reconstruct panel JSON from broken-out DB fields for UI rendering
            const sNames = [r.homepage_section_1_name, r.homepage_section_2_name, r.homepage_section_3_name, r.homepage_section_4_name].filter(Boolean);
            
            const faJson = (r.a_verdict || r.a_differentiators) ? JSON.stringify({
              score: r.score_a || 0,
              differentiators: (r.a_differentiators || '').split('; ').filter(Boolean),
              homepage_sections: [1,2,3,4].map(n => ({
                name: r[`homepage_section_${n}_name`] || '',
                finding: r[`a_section_${n}_finding`] || '',
                status: r[`a_section_${n}_status`] || ''
              })).filter(s => s.name),
              verdict: r.a_verdict || ''
            }) : '';
            
            const fbJson = (r.b_verdict || r.b_decision_maker) ? JSON.stringify({
              score: r.score_b || 0,
              decision_maker: r.b_decision_maker || '',
              strategic_outcomes: (r.b_strategic_outcomes || '').split('; ').filter(Boolean),
              outcomes: (r.b_strategic_outcomes || '').split('; ').filter(Boolean),
              homepage_sections: [1,2,3,4].map(n => ({
                name: r[`homepage_section_${n}_name`] || '',
                finding: r[`b_section_${n}_finding`] || '',
                outcome_type: r[`b_section_${n}_type`] || ''
              })).filter(s => s.name),
              verdict: r.b_verdict || ''
            }) : '';
            
            const fcJson = (r.c_verdict || r.c_section_1_orientation) ? JSON.stringify({
              score: r.score_c || 0,
              sections: [1,2,3,4].map(n => ({
                name: r[`homepage_section_${n}_name`] || '',
                orientation: r[`c_section_${n}_orientation`] || '',
                evidence: r[`c_section_${n}_evidence`] || ''
              })).filter(s => s.name),
              verdict: r.c_verdict || ''
            }) : '';
            
            const fdJson = (r.d_verdict || r.d_changes) ? JSON.stringify({
              score: r.score_d || 0,
              changes: (r.d_changes || '').split('; ').filter(Boolean).map(ch => {
                const m = ch.match(/^(.+?)\s*\((.+?)\):\s*(.+?)\s*→\s*(.+)$/);
                return m ? { name: m[1], date: m[2], before: m[3], after: m[4] } : { name: ch, date: '', before: '', after: '' };
              }),
              verdict: r.d_verdict || ''
            }) : '';
            
            const feJson = (r.e_verdict || r.e_audience_before) ? JSON.stringify({
              score: r.score_e || 0,
              before: r.e_audience_before ? (() => { const p = r.e_audience_before.split(' — '); return { buyer: p[0]||'', department: p[1]||'', market: p[2]||'' }; })() : {},
              today: r.e_audience_today ? (() => { const p = r.e_audience_today.split(' — '); return { buyer: p[0]||'', department: p[1]||'', market: p[2]||'' }; })() : {},
              verdict: r.e_verdict || ''
            }) : '';
            
            const ffJson = (r.f_verdict || r.f_products) ? JSON.stringify({
              score: r.score_f || 0,
              products: (r.f_products || '').split(', ').filter(Boolean).map(p => {
                const m = p.match(/^(.+?)\s*\((.+?)\)$/);
                return m ? { name: m[1], tag: m[2] } : { name: p, tag: 'module' };
              }),
              visitor_experience: r.f_description || '',
              description: r.f_description || '',
              verdict: r.f_verdict || ''
            }) : '';
            
            return {
              companyName: r.name, website: r.website,
              dbCompanyId: r.id, dbRunId: r.run_id, domain: r.domain,
              researchResult: r.research_raw || '', productSummary: r.product_summary || '',
              targetCustomer: r.target_customer || '', targetDecisionMaker: r.target_decision_maker || '',
              top3Outcomes: r.top3_outcomes || '', top3Differentiators: r.top3_differentiators || '',
              majorAnnouncements: r.major_announcements || '', competitors: r.competitors || '',
              customers: r.research_customers || '', funding: r.research_funding || '',
              teamSize: r.team_size || '', homepageSections: r.homepage_sections || '',
              homepageNav: r.homepage_nav || '', productPages: r.product_pages || '',
              newDirectionPage: r.new_direction_page || '', linkedinDescription: r.linkedin_description || '',
              ceoFounderName: r.ceo_founder_name || '', ceoRecentContent: r.ceo_recent_content || '',
              ceoNarrativeTheme: r.ceo_narrative_theme || '',
              newMarketingLeader: r.new_marketing_leader || '', productMarketingPeople: r.product_marketing_people || '',
              scoringResult: r.scoring_raw || '', totalScore: r.total_score || 0,
              scoreA: r.score_a || 0, scoreAJust: faJson,
              scoreB: r.score_b || 0, scoreBJust: fbJson,
              scoreC: r.score_c || 0, scoreCJust: fcJson,
              scoreD: r.score_d || 0, scoreDJust: fdJson,
              scoreE: r.score_e || 0, scoreEJust: feJson,
              scoreF: r.score_f || 0, scoreFJust: ffJson,
              scoreSummary: r.score_summary || '',
              gap1Factor: r.gap1_factor || '', gap1Name: r.gap1_name || '',
              gap1Score: r.gap1_score || 0, gap1Opportunity: r.gap1_opportunity || '',
              gap2Factor: r.gap2_factor || '', gap2Name: r.gap2_name || '',
              gap2Score: r.gap2_score || 0, gap2Opportunity: r.gap2_opportunity || '',
              icpFit: r.icp_fit === 'Disqualified' ? 'Disqualified' : ((r.total_score || 0) >= 16 ? 'Strong' : (r.total_score || 0) >= 11 ? 'Moderate' : (r.total_score || 0) >= 7 ? 'Weak' : (r.icp_fit || '')),
              disqualificationReason: r.disqualification_reason || '',
              aVerdict: r.a_verdict || '', bVerdict: r.b_verdict || '', cVerdict: r.c_verdict || '',
              dVerdict: r.d_verdict || '', eVerdict: r.e_verdict || '', fVerdict: r.f_verdict || '',
              manualScore: r.manual_score || '',
              accountStatus: r.account_status || 'Cold',
              dbStatus: r.database_status || 'active',
              lastScreenedAt: r.last_screened_at || r.run_created_at || null,
              addedAt: r.created_at || null,
              status: (r.total_score > 0 || r.icp_fit) ? 'complete' : (r.run_status || (r.run_id ? 'complete' : 'pending')), step: '', error: r.run_error || null
            };
            // Compute warnings from loaded data
            if (co.status === 'complete' && r.run_id) {
              const w = [];
              if (!r.target_decision_maker) w.push('Target decision maker not found — CTA will use generic "decision makers"');
              if (!r.competitors) w.push('Competitors not found — email cannot name specific competitors');
              if (!r.top3_differentiators) w.push('Differentiators not found');
              if (!r.ceo_narrative_theme && !r.ceo_recent_content) w.push('No CEO/founder content found — Vision Gap (G) score may be unreliable');
              if (!r.product_summary) w.push('Product summary not found');
              if (co.scoreA === 0 && !r.a_verdict) w.push('Factor A scoring data missing — re-screen recommended');
              if (co.totalScore === 0 && co.icpFit !== 'Disqualified') w.push('All scoring data missing — re-screen recommended');
              if (w.length > 0) co.screeningWarnings = w;
            }
            return co;
          });
          setCompanies(loaded);
          addLog(`Loaded ${loaded.length} companies from database`);
        }
      } catch (err) {
        addLog(`DB load error: ${err.message}`);
      }
      // Load training examples
      try {
        const examples = await getTrainingExamples();
        setTrainingExamples(examples);
        if (examples.length > 0) addLog(`Loaded ${examples.length} training examples`);
      } catch (err) {
        addLog(`Training examples load error: ${err.message}`);
      }
      // Load contacts
      try {
        const contacts = await getAllContacts();
        setAllContacts(contacts);
        if (contacts.length > 0) addLog(`Loaded ${contacts.length} contacts`);
        // Load campaign-contact mappings for filtering
        const ccMappings = await getAllCampaignContacts();
        setAllCampaignContacts(ccMappings);
      } catch (err) {
        addLog(`Contacts load error: ${err.message}`);
      }
      // Load campaigns
      try {
        const camps = await getAllCampaigns();
        setCampaigns(camps);
        if (camps.length > 0) {
          setActiveCampaignId(camps[0].id);
          addLog(`Loaded ${camps.length} campaigns`);
        }
      } catch (err) {
        addLog(`Campaigns load error: ${err.message}`);
      }
      setDbReady(true);
    };
    loadFromDb();
  }, []);

  const addLog = (msg) => setDebugLog(prev => [...prev, `${new Date().toISOString().slice(11,19)}: ${msg}`]);

  const callClaude = async (prompt, useWebSearch = false, model = "claude-haiku-4-5-20251001", systemPrompt = null) => {
    addLog(`API call (model: ${model.includes('haiku') ? 'Haiku' : 'Sonnet'}, search: ${useWebSearch})`);
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 300000);
    let response;
    try {
      response = await fetch("/api/claude", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ prompt, useWebSearch, model, maxTokens: 16000, systemPrompt }),
        signal: controller.signal
      });
    } catch (err) {
      clearTimeout(timeoutId);
      if (err.name === 'AbortError') throw new Error('API request timed out');
      throw new Error(`Network error: ${err.message}`);
    }
    clearTimeout(timeoutId);
    if (!response.ok) {
      const errorText = await response.text().catch(() => 'Unknown error');
      throw new Error(`API error (${response.status}): ${errorText.slice(0, 200)}`);
    }
    const data = await response.json();
    if (data.error) throw new Error(data.error);
    const usage = data.usage || {};
    addLog(`Tokens - In: ${usage.input_tokens || 0}, Out: ${usage.output_tokens || 0}`);
    if (data.content && Array.isArray(data.content)) {
      return data.content.filter(b => b.type === 'text').map(b => b.text).join('\n');
    }
    return '';
  };

  // === EXA API HELPERS ===

  const exaSearch = async (query, options = {}) => {
    const { category, numResults = 10, contents, startPublishedDate, endPublishedDate, includeDomains, excludeDomains } = options;
    try {
      const resp = await fetch('/api/exa', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query, category, numResults, contents, startPublishedDate, endPublishedDate, includeDomains, excludeDomains }),
        signal: AbortSignal.timeout(60000)
      });
      if (!resp.ok) {
        const err = await resp.json().catch(() => ({}));
        throw new Error(err.error || `Exa search error: ${resp.status}`);
      }
      const data = await resp.json();
      return data.results || [];
    } catch (e) {
      addLog(`Exa search error (${query.slice(0, 40)}): ${e.message}`);
      return [];
    }
  };

  const exaContents = async (urls, options = {}) => {
    const { text, highlights, summary, subpages, subpageTarget, maxAgeHours, livecrawlTimeout } = options;
    try {
      const resp = await fetch('/api/exa-contents', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ urls, text, highlights, summary, subpages, subpageTarget, maxAgeHours, livecrawlTimeout }),
        signal: AbortSignal.timeout(60000)
      });
      if (!resp.ok) {
        const err = await resp.json().catch(() => ({}));
        throw new Error(err.error || `Exa contents error: ${resp.status}`);
      }
      const data = await resp.json();
      return data.results || [];
    } catch (e) {
      addLog(`Exa contents error: ${e.message}`);
      return [];
    }
  };

  const formatExaResults = (results, mode = 'full') => {
    if (!results || results.length === 0) return 'No results found.';
    return results.map((r, i) => {
      let content = '';
      if (r.text) content = r.text;
      else if (r.highlights && r.highlights.length > 0) content = r.highlights.join('\n');
      else if (r.summary) content = r.summary;
      const date = r.publishedDate ? ` (${r.publishedDate.slice(0, 10)})` : '';
      return `[${i + 1}] ${r.title || 'Untitled'}${date}\nURL: ${r.url || ''}\n${content}`;
    }).join('\n\n---\n\n');
  };

  const gatherExaResearch = async (companyName, website, updateStep) => {
    const domain = website.replace(/^https?:\/\//, '').replace(/^www\./, '').replace(/\/$/, '');
    const fullUrl = website.startsWith('http') ? website : `https://${website}`;
    const twoYearsAgo = new Date(Date.now() - 730 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);

    // Run all Exa searches in parallel for speed
    updateStep('Step 1a: Gathering data via Exa...');
    addLog(`[${companyName}] Running Exa searches in parallel...`);

    const [
      homepageResults,
      newsResults,
      competitorResults,
      caseStudyResults,
      fundingResults,
      linkedinResults,
      tweetResults,
      ceoContentResults,
    ] = await Promise.all([
      // 1. Homepage + product pages via contents crawl
      exaContents([fullUrl], {
        text: { maxCharacters: 12000 },
        subpages: 5,
        subpageTarget: ['product', 'platform', 'solutions', 'pricing', 'about'],
        maxAgeHours: 24,
        livecrawlTimeout: 12000
      }),

      // 2. News & announcements
      exaSearch(`${companyName} product launch announcement partnership`, {
        category: 'news',
        numResults: 10,
        startPublishedDate: twoYearsAgo,
        contents: {
          highlights: {
            query: 'product launch acquisition partnership rebrand new feature pivot',
            maxCharacters: 3000
          }
        }
      }),

      // 3. Competitor comparisons & reviews
      exaSearch(`${companyName} vs competitors comparison review`, {
        numResults: 8,
        contents: {
          highlights: {
            query: 'differentiator unique advantage capability comparison alternative',
            maxCharacters: 3000
          }
        }
      }),

      // 4. Case studies & customer outcomes
      exaSearch(`${companyName} case study customer results`, {
        numResults: 8,
        contents: {
          highlights: {
            query: 'results ROI reduced increased saved revenue cost metrics percentage',
            maxCharacters: 3000
          }
        }
      }),

      // 5. Funding & company facts
      exaSearch(`${companyName} funding round series investors team size`, {
        numResults: 5,
        contents: {
          highlights: {
            query: 'raised funding series investors valuation team employees headcount',
            maxCharacters: 2000
          }
        }
      }),

      // 6. LinkedIn company description
      exaSearch(`${companyName} company LinkedIn about`, {
        numResults: 3,
        includeDomains: ['linkedin.com'],
        contents: {
          text: { maxCharacters: 3000 }
        }
      }),

      // 7. CEO/founder tweets
      exaSearch(`${companyName} CEO founder`, {
        category: 'tweet',
        numResults: 10,
        contents: {
          text: { maxCharacters: 1500 }
        }
      }),

      // 8. CEO blog/podcast/conference content
      exaSearch(`${companyName} CEO founder vision strategy direction`, {
        numResults: 5,
        startPublishedDate: new Date(Date.now() - 180 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10),
        contents: {
          highlights: {
            query: 'company vision strategy direction product roadmap future',
            maxCharacters: 3000
          }
        }
      }),
    ]);

    addLog(`[${companyName}] Exa data gathered: homepage=${homepageResults.length}, news=${newsResults.length}, competitors=${competitorResults.length}, cases=${caseStudyResults.length}, funding=${fundingResults.length}, linkedin=${linkedinResults.length}, tweets=${tweetResults.length}, ceo=${ceoContentResults.length}`);

    // Format homepage content — main page + subpages
    let homepageContent = '';
    let productPagesContent = '';
    if (homepageResults.length > 0) {
      const main = homepageResults[0];
      homepageContent = main.text || '';
      // Subpages come as additional results or in subpages array
      const subpages = main.subpages || homepageResults.slice(1);
      if (subpages.length > 0) {
        productPagesContent = subpages.map(sp => {
          return `PAGE: ${sp.title || sp.url}\nURL: ${sp.url}\n${sp.text || sp.summary || ''}`;
        }).join('\n\n---\n\n');
      }
    }

    // Build the compiled research context
    const exaContext = `=== HOMEPAGE CONTENT (crawled from ${fullUrl} via Exa) ===
${homepageContent || 'NOT AVAILABLE — Exa could not crawl this page.'}
=== END HOMEPAGE CONTENT ===

=== PRODUCT / SUBPAGES (crawled from links on homepage) ===
${productPagesContent || 'No subpages found.'}
=== END PRODUCT / SUBPAGES ===

=== NEWS & ANNOUNCEMENTS (last 24 months) ===
${formatExaResults(newsResults)}
=== END NEWS ===

=== COMPETITOR COMPARISONS & REVIEWS ===
${formatExaResults(competitorResults)}
=== END COMPETITORS ===

=== CASE STUDIES & CUSTOMER OUTCOMES ===
${formatExaResults(caseStudyResults)}
=== END CASE STUDIES ===

=== FUNDING & COMPANY FACTS ===
${formatExaResults(fundingResults)}
=== END FUNDING ===

=== LINKEDIN COMPANY INFO ===
${formatExaResults(linkedinResults)}
=== END LINKEDIN ===

=== CEO/FOUNDER TWEETS ===
${formatExaResults(tweetResults)}
=== END TWEETS ===

=== CEO/FOUNDER BLOG, PODCAST & CONFERENCE CONTENT (last 6 months) ===
${formatExaResults(ceoContentResults)}
=== END CEO CONTENT ===`;

    return { exaContext, homepageContent };
  };

  // Legacy fetchPage kept as fallback (unused in normal flow)
  const fetchPage = async (url) => {
    try {
      const resp = await fetch('/api/fetch-page', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ url: url.startsWith('http') ? url : `https://${url}` })
      });
      if (resp.ok) {
        const data = await resp.json();
        return data.text || '';
      }
    } catch (e) { /* ignore */ }
    return '';
  };

  const parseCSV = (text) => {
    const lines = text.trim().split('\n');
    const headers = lines[0].split(',').map(h => h.trim().toLowerCase().replace(/^"|"$/g, ''));
    const nameIdx = headers.findIndex(h => h === 'company' || h === 'company name' || h === 'company_name' || h === 'name');
    const websiteIdx = headers.findIndex(h => h.includes('website') || h.includes('url') || h.includes('homepage') || h.includes('domain'));
    const manualScoreIdx = headers.findIndex(h => h === 'manual score' || h === 'manual_score' || h === 'my score' || h === 'my_score');
    if (websiteIdx === -1) throw new Error('CSV must have a Website/URL column.');
    const results = [];
    for (let i = 1; i < lines.length; i++) {
      const values = [];
      let current = '';
      let inQuotes = false;
      const line = lines[i];
      if (!line.trim()) continue;
      for (let j = 0; j < line.length; j++) {
        const char = line[j];
        if (char === '"') { inQuotes = !inQuotes; }
        else if (char === ',' && !inQuotes) { values.push(current.trim().replace(/^"|"$/g, '')); current = ''; }
        else { current += char; }
      }
      values.push(current.trim().replace(/^"|"$/g, ''));
      const website = (values[websiteIdx] || '').trim();
      if (!website) continue;
      let companyName = nameIdx !== -1 ? (values[nameIdx] || '').trim() : '';
      if (!companyName) {
        try {
          const url = website.startsWith('http') ? website : `https://${website}`;
          companyName = new URL(url).hostname.replace(/^www\./, '').split('.')[0];
          companyName = companyName.charAt(0).toUpperCase() + companyName.slice(1);
        } catch { companyName = website; }
      }
      results.push({
        companyName, website,
        researchResult: '', productSummary: '', targetCustomer: '', targetDecisionMaker: '',
        top3Outcomes: '', top3Differentiators: '', majorAnnouncements: '', competitors: '',
        customers: '', funding: '', teamSize: '', homepageSections: '',
        homepageNav: '', productPages: '', newDirectionPage: '', linkedinDescription: '',
        ceoFounderName: '', ceoRecentContent: '', ceoNarrativeTheme: '',
        newMarketingLeader: '', productMarketingPeople: '',
        scoringResult: '',
        totalScore: 0,
        scoreA: 0, scoreAJust: '',
        scoreB: 0, scoreBJust: '',
        scoreC: 0, scoreCJust: '',
        scoreD: 0, scoreDJust: '',
        scoreE: 0, scoreEJust: '',
        scoreF: 0, scoreFJust: '',
        scoreG: 0, scoreGJust: '',
        scoreSummary: '',
        gap1Factor: '', gap1Name: '', gap1Score: 0, gap1Opportunity: '',
        gap2Factor: '', gap2Name: '', gap2Score: 0, gap2Opportunity: '',
        icpFit: '',
        disqualificationReason: '',
        manualScore: manualScoreIdx !== -1 ? (values[manualScoreIdx] || '').trim() : '',
        accountStatus: 'Cold',
        dbStatus: 'screened',
        status: 'pending', step: '', error: null
      });
    }
    return results;
  };

  const handleFileUpload = (e) => {
    const file = e.target.files[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = async (event) => {
      try {
        const parsed = parseCSV(event.target.result);
        // Upsert each company to Supabase and attach DB IDs
        if (supabase) {
          for (const co of parsed) {
            const domain = normalizeDomain(co.website);
            co.domain = domain;
            try {
              const dbCo = await dbUpsertCompany(domain, co.companyName, co.website, 'screened');
              if (dbCo) {
                co.dbCompanyId = dbCo.id;
                // Check if already scored — keep existing data
                const existing = companies.find(c => c.domain === domain && c.status === 'complete');
                if (existing) {
                  Object.assign(co, existing);
                }
              }
            } catch (err) {
              addLog(`DB upsert error for ${co.companyName}: ${err.message}`);
            }
          }
        }
        // Merge with existing companies (don't overwrite already-loaded ones)
        setCompanies(prev => {
          const domainMap = new Map(prev.map(c => [normalizeDomain(c.website), c]));
          for (const co of parsed) {
            const d = normalizeDomain(co.website);
            if (!domainMap.has(d) || domainMap.get(d).status !== 'complete') {
              domainMap.set(d, co);
            }
          }
          return Array.from(domainMap.values());
        });
        setError('');
        setSelectedCompany(null);
        addLog(`Loaded ${parsed.length} companies from CSV`);
      } catch (err) { setError(err.message); }
    };
    reader.readAsText(file);
    e.target.value = '';
  };

  const parseField = (text, field) => {
    // Try plain format first: FIELD_NAME: content
    const plainPattern = new RegExp(`${field}:\\s*([\\s\\S]*?)(?=\\n[A-Z_]{3,}:|$)`, 'i');
    const plainMatch = text.match(plainPattern);
    if (plainMatch && plainMatch[1].trim()) return plainMatch[1].trim();
    
    // Try markdown header format: ### FIELD_NAME\n content (until next ### or ---)
    const mdPattern = new RegExp(`###\\s*${field}\\s*\\n([\\s\\S]*?)(?=\\n###\\s|\\n---\\s|\\n##\\s|$)`, 'i');
    const mdMatch = text.match(mdPattern);
    if (mdMatch && mdMatch[1].trim()) return mdMatch[1].trim();
    
    // Try with **FIELD_NAME** bold format
    const boldPattern = new RegExp(`\\*\\*${field}\\*\\*[:\\s]*([\\s\\S]*?)(?=\\n\\*\\*[A-Z_]{3,}\\*\\*|\\n###|\\n---|$)`, 'i');
    const boldMatch = text.match(boldPattern);
    if (boldMatch && boldMatch[1].trim()) return boldMatch[1].trim();
    
    return '';
  };

  const processCompany = async (index) => {
    const company = companies[index];
    const { companyName, website } = company;
    const updateCompany = (fields) => {
      setCompanies(prev => { const u = [...prev]; u[index] = { ...u[index], ...fields }; return u; });
    };
    
    // Ensure company has a DB record (domain is the primary key)
    if (supabase && !company.dbCompanyId) {
      try {
        const domain = company.domain || normalizeDomain(website);
        const dbCo = await dbUpsertCompany(domain, companyName, website, 'screened');
        if (dbCo) {
          updateCompany({ dbCompanyId: dbCo.id, domain });
          company.dbCompanyId = dbCo.id;
          company.domain = domain;
        }
      } catch (err) {
        addLog(`[${companyName}] DB upsert error: ${err.message}`);
      }
    }
    
    updateCompany({ status: 'processing', step: 'Gathering data via Exa...' });
    try {
      // STEP 1a: GATHER DATA VIA EXA (parallel searches)
      addLog(`[${companyName}] Step 1a: Exa data gathering...`);
      const { exaContext, homepageContent } = await gatherExaResearch(companyName, website, (step) => updateCompany({ step }));
      addLog(`[${companyName}] Exa context: ${exaContext.length} chars, Homepage: ${homepageContent.length} chars`);

      // Check if homepage was available — if not, disqualify and skip
      if (!homepageContent || homepageContent.trim().length < 50) {
        addLog(`[${companyName}] Homepage not available — disqualifying`);
        updateCompany({
          status: 'complete',
          step: 'Disqualified: Homepage not available',
          icpFit: 'Disqualified',
          scoreSummary: 'Could not retrieve homepage content. Screening requires homepage analysis to score narrative gaps.',
          totalScore: 0,
        });
        if (supabase && company.dbCompanyId) {
          try {
            let runId = company.dbRunId;
            if (!runId) {
              const run = await createResearchRun(company.dbCompanyId);
              if (run) { runId = run.id; updateCompany({ dbRunId: runId }); }
            }
            if (runId) {
              await updateResearchRun(runId, {
                status: 'complete',
                research_raw: 'Homepage not available — disqualified.',
                scoring_raw: JSON.stringify({ total_score: 0, icp_fit: 'Disqualified', disqualification_reason: 'Homepage not available', summary: 'Could not retrieve homepage content.' }),
              });
            }
            await dbUpdateCompany(company.dbCompanyId, {
              total_score: 0,
              icp_fit: 'Disqualified',
              score_summary: 'Could not retrieve homepage content.',
            });
          } catch (err) { addLog(`[${companyName}] DB save disqualification error: ${err.message}`); }
        }
        return;
      }

      // STEP 1b: SYNTHESIZE WITH CLAUDE (no web search needed)
      updateCompany({ step: 'Step 1b: Synthesizing research...' });
      addLog(`[${companyName}] Step 1b: Synthesis (Haiku, no web search)`);
      const researchPromptFull = `Synthesize the following pre-gathered research data into a structured report.\n\nCompany: ${companyName}\nWebsite: ${website}\n\n${exaContext}\n\n${researchPrompt}`;
      const researchResult = await callClaude(researchPromptFull, false, "claude-haiku-4-5-20251001");
      const researchFields = {
        researchResult,
        productSummary: parseField(researchResult, 'PRODUCT_SUMMARY'),
        targetCustomer: parseField(researchResult, 'TARGET_CUSTOMER'),
        targetDecisionMaker: parseField(researchResult, 'TARGET_DECISION_MAKER'),
        top3Outcomes: parseField(researchResult, 'TOP_3_OUTCOMES'),
        top3Differentiators: parseField(researchResult, 'TOP_3_DIFFERENTIATORS'),
        majorAnnouncements: parseField(researchResult, 'MAJOR_ANNOUNCEMENTS'),
        competitors: parseField(researchResult, 'COMPETITORS'),
        customers: parseField(researchResult, 'COMPANY_CUSTOMERS'),
        funding: parseField(researchResult, 'COMPANY_FUNDING'),
        teamSize: parseField(researchResult, 'COMPANY_TEAM_SIZE'),
        homepageSections: parseField(researchResult, 'HOMEPAGE_SECTIONS'),
        homepageNav: parseField(researchResult, 'HOMEPAGE_NAVIGATION'),
        productPages: parseField(researchResult, 'PRODUCT_PAGES'),
        newDirectionPage: parseField(researchResult, 'NEW_DIRECTION_PAGE'),
        linkedinDescription: parseField(researchResult, 'LINKEDIN_COMPANY_DESCRIPTION'),
        ceoFounderName: parseField(researchResult, 'CEO_FOUNDER_NAME'),
        ceoRecentContent: parseField(researchResult, 'CEO_RECENT_CONTENT'),
        ceoNarrativeTheme: parseField(researchResult, 'CEO_NARRATIVE_THEME'),
        newMarketingLeader: parseField(researchResult, 'NEW_MARKETING_LEADER'),
        productMarketingPeople: parseField(researchResult, 'PRODUCT_MARKETING_PEOPLE'),
      };
      updateCompany(researchFields);
      addLog(`[${companyName}] Research synthesis complete`);

      // Collect screening warnings from research
      const warnings = [];
      if (!researchFields.productSummary) warnings.push('Product summary not found');
      if (!researchFields.targetDecisionMaker) warnings.push('Target decision maker not found — CTA will use generic "decision makers"');
      if (!researchFields.competitors) warnings.push('Competitors not found — email cannot name specific competitors');
      if (!researchFields.top3Differentiators) warnings.push('Differentiators not found');
      if (!researchFields.top3Outcomes) warnings.push('Customer outcomes not found');
      if (!researchFields.homepageSections) warnings.push('Homepage sections not parsed');
      if (!researchFields.ceoNarrativeTheme && !researchFields.ceoRecentContent) warnings.push('No CEO/founder content found — Vision Gap (G) score may be unreliable');
      if (warnings.length > 0) {
        updateCompany({ step: `Research complete (${warnings.length} warning${warnings.length > 1 ? 's' : ''})` });
        warnings.forEach(w => addLog(`[${companyName}] ⚠ ${w}`));
      }

      // Persist research to Supabase
      let runId = company.dbRunId;
      if (supabase && company.dbCompanyId) {
        try {
          if (!runId) {
            const run = await createResearchRun(company.dbCompanyId);
            if (run) { runId = run.id; updateCompany({ dbRunId: runId }); }
          }
          if (runId) {
            await updateResearchRun(runId, {
              status: 'scoring',
              research_raw: researchResult,
              product_summary: researchFields.productSummary,
              target_customer: researchFields.targetCustomer,
              target_decision_maker: researchFields.targetDecisionMaker,
              top3_outcomes: researchFields.top3Outcomes,
              top3_differentiators: researchFields.top3Differentiators,
              major_announcements: researchFields.majorAnnouncements,
              competitors: researchFields.competitors,
              customers: researchFields.customers,
              funding: researchFields.funding,
              team_size: researchFields.teamSize,
              homepage_sections: researchFields.homepageSections,
              homepage_nav: researchFields.homepageNav,
              product_pages: researchFields.productPages,
              new_direction_page: researchFields.newDirectionPage,
              linkedin_description: researchFields.linkedinDescription,
              ceo_founder_name: researchFields.ceoFounderName,
              ceo_recent_content: researchFields.ceoRecentContent,
              ceo_narrative_theme: researchFields.ceoNarrativeTheme,
              new_marketing_leader: researchFields.newMarketingLeader,
              product_marketing_people: researchFields.productMarketingPeople,
            });
          }
        } catch (err) { addLog(`[${companyName}] DB save research error: ${err.message}`); }
      }

      // STEP 2: SCORING
      updateCompany({ step: 'Step 2: Scoring...' });
      addLog(`[${companyName}] Step 2: Scoring (Sonnet)`);
      
      // === STEP 3: PARALLEL FACTOR SCORING (7 Haiku calls) ===
      updateCompany({ step: 'Scoring factors (7 parallel)...' });
      addLog(`[${companyName}] Starting parallel factor scoring...`);

      const parseFactorJSON = (raw) => {
        try {
          let s = raw.replace(/^[\s\S]*?\`\`\`(?:json)?\s*/i, '').replace(/\s*\`\`\`[\s\S]*$/i, '');
          if (!s.trim().startsWith('{')) { const a = s.indexOf('{'), b = s.lastIndexOf('}'); if (a !== -1 && b !== -1) s = s.slice(a, b + 1); }
          return JSON.parse(s.trim());
        } catch { return null; }
      };

      const hp = researchFields.homepageSections || '';
      const ceoTheme = researchFields.ceoNarrativeTheme || '';
      const ceoContent = researchFields.ceoRecentContent || '';
      const diffCtx = `Company: ${companyName}\n\nTOP_3_DIFFERENTIATORS:\n${researchFields.top3Differentiators || 'None'}\n\nCOMPETITORS:\n${researchFields.competitors || 'None'}\n\nCEO_NARRATIVE_THEME:\n${ceoTheme}\n\nHOMEPAGE_SECTIONS:\n${hp}`;
      const outCtx = `Company: ${companyName}\n\nTOP_3_OUTCOMES:\n${researchFields.top3Outcomes || 'None'}\n\nTARGET_DECISION_MAKER:\n${researchFields.targetDecisionMaker || 'Unknown'}\n\nCEO_NARRATIVE_THEME:\n${ceoTheme}\n\nHOMEPAGE_SECTIONS:\n${hp}`;
      const custCtx = `Company: ${companyName}\n\nCEO_NARRATIVE_THEME:\n${ceoTheme}\n\nHOMEPAGE_SECTIONS:\n${hp}`;
      const prodCtx = `Company: ${companyName}\n\nMAJOR_ANNOUNCEMENTS:\n${researchFields.majorAnnouncements || 'None'}\n\nCEO_RECENT_CONTENT:\n${ceoContent}\n\nCEO_NARRATIVE_THEME:\n${ceoTheme}\n\nHOMEPAGE_SECTIONS:\n${hp}`;
      const audCtx = `Company: ${companyName}\n\nMAJOR_ANNOUNCEMENTS:\n${researchFields.majorAnnouncements || 'None'}\n\nCEO_RECENT_CONTENT:\n${ceoContent}\n\nTARGET_CUSTOMER:\n${researchFields.targetCustomer || 'Unknown'}\n\nPRODUCT_PAGES:\n${researchFields.productPages || 'None'}`;
      const multiCtx = `Company: ${companyName}\n\nPRODUCT_PAGES:\n${researchFields.productPages || 'None'}\n\nHOMEPAGE_NAVIGATION:\n${researchFields.homepageNav || 'None'}\n\nHOMEPAGE_SECTIONS:\n${hp}`;
      const visCtx = `Company: ${companyName}\n\nCEO_NARRATIVE_THEME:\n${ceoTheme}\n\nCEO_RECENT_CONTENT:\n${ceoContent}\n\nHOMEPAGE_SECTIONS:\n${hp}`;
      const disqCtx = `Company: ${companyName}\nWebsite: ${website}\n\nPRODUCT_SUMMARY:\n${researchFields.productSummary || 'Unknown'}\n\nTARGET_CUSTOMER:\n${researchFields.targetCustomer || 'Unknown'}\n\nFUNDING:\n${researchFields.funding || 'Unknown'}\n\nTEAM_SIZE:\n${researchFields.teamSize || 'Unknown'}`;

      addLog(`[${companyName}] Disq context: productSummary=${(researchFields.productSummary || 'EMPTY').substring(0, 80)}... targetCustomer=${(researchFields.targetCustomer || 'EMPTY').substring(0, 50)}...`);

      const [disqRaw, faRaw, fbRaw, fcRaw, fdRaw, feRaw, ffRaw, fgRaw] = await Promise.all([
        callClaude(disqCtx + '\n\n' + scoringPrompts.disqualification, false, 'claude-haiku-4-5-20251001', scoringPrompts.system),
        callClaude(diffCtx + '\n\n' + scoringPrompts.A, false, 'claude-haiku-4-5-20251001', scoringPrompts.system),
        callClaude(outCtx + '\n\n' + scoringPrompts.B, false, 'claude-haiku-4-5-20251001', scoringPrompts.system),
        callClaude(custCtx + '\n\n' + scoringPrompts.C, false, 'claude-haiku-4-5-20251001', scoringPrompts.system),
        callClaude(prodCtx + '\n\n' + scoringPrompts.D, false, 'claude-haiku-4-5-20251001', scoringPrompts.system),
        callClaude(audCtx + '\n\n' + scoringPrompts.E, false, 'claude-haiku-4-5-20251001', scoringPrompts.system),
        callClaude(multiCtx + '\n\n' + scoringPrompts.F, false, 'claude-haiku-4-5-20251001', scoringPrompts.system),
        callClaude(visCtx + '\n\n' + scoringPrompts.G, false, 'claude-haiku-4-5-20251001', scoringPrompts.system),
      ]);
      addLog(`[${companyName}] All 8 scoring calls complete`);

      const disqData = parseFactorJSON(disqRaw);
      const fa = parseFactorJSON(faRaw) || {};
      const fb = parseFactorJSON(fbRaw) || {};
      const fc = parseFactorJSON(fcRaw) || {};
      const fd = parseFactorJSON(fdRaw) || {};
      const fe = parseFactorJSON(feRaw) || {};
      const ff = parseFactorJSON(ffRaw) || {};
      const fg = parseFactorJSON(fgRaw) || {};

      let icpFit, totalScore;
      if (disqData?.disqualified) {
        icpFit = 'Disqualified'; totalScore = 0;
        updateCompany({ icpFit: 'Disqualified', disqualificationReason: disqData.reason || 'Disqualified',
          scoringResult: JSON.stringify({ disqualified: true, reason: disqData.reason }),
          status: 'complete', step: '', error: null });
        if (supabase && runId) {
          try { await updateResearchRun(runId, { status: 'complete', scoring_raw: JSON.stringify({ disqualified: true, reason: disqData.reason }), total_score: 0, icp_fit: 'Disqualified', disqualification_reason: disqData.reason || 'Disqualified' }); }
          catch (err) { addLog(`[${companyName}] DB save error: ${err.message}`); }
        }
        addLog(`[${companyName}] Disqualified: ${disqData.reason}`);
        return;
      }

      const scores = { A: fa.score || 1, B: fb.score || 1, C: fc.score || 1, D: fd.score || 1, E: fe.score || 1, F: ff.score || 1, G: fg.score || 1 };
      totalScore = Object.values(scores).reduce((a, b) => a + b, 0);
      icpFit = totalScore >= 16 ? 'Strong' : totalScore >= 11 ? 'Moderate' : 'Weak';

      const sortedGaps = Object.entries(scores).sort((a, b) => b[1] - a[1] || GAP_TIEBREAKER.indexOf(a[0]) - GAP_TIEBREAKER.indexOf(b[0]));
      const gap1Key = sortedGaps[0][0];
      const gap2Key = sortedGaps[1][0];

      const combinedScoring = { total_score: totalScore, icp_fit: icpFit, disqualification_reason: 'None',
        summary: `${companyName} scored ${totalScore}/21 (${icpFit}). Top gaps: ${gap1Key}. ${FACTOR_NAMES[gap1Key]} (+${scores[gap1Key]}), ${gap2Key}. ${FACTOR_NAMES[gap2Key]} (+${scores[gap2Key]}).`,
        factor_a: fa, factor_b: fb, factor_c: fc, factor_d: fd, factor_e: fe, factor_f: ff, factor_g: fg };
      const scoringResult = JSON.stringify(combinedScoring);

      const aSections = fa.homepage_sections || [];
      const sectionNames = aSections.map(s => s.name || '');
      const bSections = fb.homepage_sections || [];
      const cSections = fc.sections || [];
      const dChanges = (fd.changes || []).map(ch => `${ch.name} (${ch.date}): ${ch.before} \u2192 ${ch.after}`).join('; ');
      const fProducts = (ff.products || []).map(p => `${p.name} (${p.tag})`).join(', ');

      const scoreFields = {
        scoringResult, totalScore, icpFit,
        scoreSummary: combinedScoring.summary, disqualificationReason: 'None',
        gap1Factor: gap1Key, gap1Name: FACTOR_NAMES[gap1Key], gap1Score: scores[gap1Key], gap1Opportunity: '',
        gap2Factor: gap2Key, gap2Name: FACTOR_NAMES[gap2Key], gap2Score: scores[gap2Key], gap2Opportunity: '',
        scoreAJust: JSON.stringify(fa), scoreBJust: JSON.stringify(fb), scoreCJust: JSON.stringify(fc),
        scoreDJust: JSON.stringify(fd), scoreEJust: JSON.stringify(fe), scoreFJust: JSON.stringify(ff), scoreGJust: JSON.stringify(fg),
        hpSection1Name: sectionNames[0] || '', hpSection2Name: sectionNames[1] || '', hpSection3Name: sectionNames[2] || '', hpSection4Name: sectionNames[3] || '',
        scoreA: fa.score || 0, aDifferentiators: (fa.differentiators || []).join('; '),
        aSection1Finding: aSections[0]?.finding || '', aSection1Status: aSections[0]?.status || '',
        aSection2Finding: aSections[1]?.finding || '', aSection2Status: aSections[1]?.status || '',
        aSection3Finding: aSections[2]?.finding || '', aSection3Status: aSections[2]?.status || '',
        aSection4Finding: aSections[3]?.finding || '', aSection4Status: aSections[3]?.status || '',
        aVerdict: fa.verdict || '',
        scoreB: fb.score || 0, bDecisionMaker: fb.decision_maker || '', bOutcomes: (fb.outcomes || []).join('; '),
        bSection1Finding: bSections[0]?.finding || '', bSection1Type: bSections[0]?.outcome_type || '',
        bSection2Finding: bSections[1]?.finding || '', bSection2Type: bSections[1]?.outcome_type || '',
        bSection3Finding: bSections[2]?.finding || '', bSection3Type: bSections[2]?.outcome_type || '',
        bSection4Finding: bSections[3]?.finding || '', bSection4Type: bSections[3]?.outcome_type || '',
        bVerdict: fb.verdict || '',
        scoreC: fc.score || 0,
        cSection1Orientation: cSections[0]?.orientation || '', cSection1Evidence: cSections[0]?.evidence || '',
        cSection2Orientation: cSections[1]?.orientation || '', cSection2Evidence: cSections[1]?.evidence || '',
        cSection3Orientation: cSections[2]?.orientation || '', cSection3Evidence: cSections[2]?.evidence || '',
        cSection4Orientation: cSections[3]?.orientation || '', cSection4Evidence: cSections[3]?.evidence || '',
        cVerdict: fc.verdict || '',
        scoreD: fd.score || 0, dChanges, dVerdict: fd.verdict || '',
        scoreE: fe.score || 0,
        eAudienceBefore: fe.before ? `${fe.before.buyer} \u2014 ${fe.before.department} \u2014 ${fe.before.market}` : '',
        eAudienceToday: fe.today ? `${fe.today.buyer} \u2014 ${fe.today.department} \u2014 ${fe.today.market}` : '',
        eConfidence: fe.confidence || '', eConfidenceReason: fe.confidence_reason || '', eVerdict: fe.verdict || '',
        scoreF: ff.score || 0, fProducts, fVisitorExperience: ff.visitor_experience || ff.description || '', fVerdict: ff.verdict || '',
        scoreG: fg.score || 0, gCeoNarrative: fg.ceo_narrative || '', gHomepageNarrative: fg.homepage_narrative || '', gVerdict: fg.verdict || '',
        status: 'complete', step: '', error: null
      };

      // Add scoring warnings
      if (!fa.verdict) warnings.push('Factor A (Differentiation) scoring failed — no verdict returned');
      if (!fb.verdict) warnings.push('Factor B (Outcomes) scoring failed — no verdict returned');
      if (!fc.verdict) warnings.push('Factor C (Customer-centric) scoring failed — no verdict returned');
      if (!fd.verdict) warnings.push('Factor D (Product Change) scoring failed — no verdict returned');
      if (!fe.verdict) warnings.push('Factor E (Audience Change) scoring failed — no verdict returned');
      if (!ff.verdict) warnings.push('Factor F (Multi-product) scoring failed — no verdict returned');
      if (!fg.verdict) warnings.push('Factor G (Vision Gap) scoring failed — no verdict returned');
      if (fg.score === 0 && !fg.ceo_narrative) warnings.push('Vision Gap scored 0 with no CEO narrative — likely missing CEO data');

      scoreFields.screeningWarnings = warnings;
      updateCompany(scoreFields);
      addLog(`[${companyName}] Complete: Score: ${totalScore}/21, Fit: ${icpFit}${warnings.length > 0 ? ` (${warnings.length} warnings)` : ''}`);

      // Persist scoring to Supabase
      if (supabase && runId) {
        try {
          await updateResearchRun(runId, {
            status: 'complete',
            scoring_raw: scoringResult,
            total_score: totalScore,
            score_summary: scoreFields.scoreSummary || '',
            icp_fit: icpFit,
            disqualification_reason: scoreFields.disqualificationReason || '',
            
            homepage_section_1_name: scoreFields.hpSection1Name || '',
            homepage_section_2_name: scoreFields.hpSection2Name || '',
            homepage_section_3_name: scoreFields.hpSection3Name || '',
            homepage_section_4_name: scoreFields.hpSection4Name || '',
            
            score_a: scoreFields.scoreA,
            a_differentiators: scoreFields.aDifferentiators || '',
            a_section_1_finding: scoreFields.aSection1Finding || '', a_section_1_status: scoreFields.aSection1Status || '',
            a_section_2_finding: scoreFields.aSection2Finding || '', a_section_2_status: scoreFields.aSection2Status || '',
            a_section_3_finding: scoreFields.aSection3Finding || '', a_section_3_status: scoreFields.aSection3Status || '',
            a_section_4_finding: scoreFields.aSection4Finding || '', a_section_4_status: scoreFields.aSection4Status || '',
            a_verdict: scoreFields.aVerdict || '',
            
            score_b: scoreFields.scoreB,
            b_decision_maker: scoreFields.bDecisionMaker || '',
            b_strategic_outcomes: scoreFields.bOutcomes || '',
            b_tactical_outcomes: '',
            b_section_1_finding: scoreFields.bSection1Finding || '', b_section_1_type: scoreFields.bSection1Type || '',
            b_section_2_finding: scoreFields.bSection2Finding || '', b_section_2_type: scoreFields.bSection2Type || '',
            b_section_3_finding: scoreFields.bSection3Finding || '', b_section_3_type: scoreFields.bSection3Type || '',
            b_section_4_finding: scoreFields.bSection4Finding || '', b_section_4_type: scoreFields.bSection4Type || '',
            b_verdict: scoreFields.bVerdict || '',
            
            score_c: scoreFields.scoreC,
            c_section_1_orientation: scoreFields.cSection1Orientation || '', c_section_1_evidence: scoreFields.cSection1Evidence || '',
            c_section_2_orientation: scoreFields.cSection2Orientation || '', c_section_2_evidence: scoreFields.cSection2Evidence || '',
            c_section_3_orientation: scoreFields.cSection3Orientation || '', c_section_3_evidence: scoreFields.cSection3Evidence || '',
            c_section_4_orientation: scoreFields.cSection4Orientation || '', c_section_4_evidence: scoreFields.cSection4Evidence || '',
            c_verdict: scoreFields.cVerdict || '',
            
            score_d: scoreFields.scoreD,
            d_changes: scoreFields.dChanges || '',
            d_verdict: scoreFields.dVerdict || '',
            
            score_e: scoreFields.scoreE,
            e_audience_before: scoreFields.eAudienceBefore || '',
            e_audience_today: scoreFields.eAudienceToday || '',
            e_verdict: scoreFields.eVerdict || '',
            
            score_f: scoreFields.scoreF,
            f_products: scoreFields.fProducts || '',
            f_description: scoreFields.fVisitorExperience || '',
            f_verdict: scoreFields.fVerdict || '',
            
            score_g: scoreFields.scoreG,
            g_ceo_narrative: scoreFields.gCeoNarrative || '',
            g_homepage_narrative: scoreFields.gHomepageNarrative || '',
            g_gap: '',
            g_verdict: scoreFields.gVerdict || '',
            
            gap1_factor: scoreFields.gap1Factor || '',
            gap1_name: scoreFields.gap1Name || '',
            gap1_score: scoreFields.gap1Score || 0,
            gap1_opportunity: scoreFields.gap1Opportunity || '',
            gap2_factor: scoreFields.gap2Factor || '',
            gap2_name: scoreFields.gap2Name || '',
            gap2_score: scoreFields.gap2Score || 0,
            gap2_opportunity: scoreFields.gap2Opportunity || '',
          });
          // Update last_screened_at on the company
          if (company.dbCompanyId) {
            await dbUpdateCompany(company.dbCompanyId, { last_screened_at: new Date().toISOString() });
          }
        } catch (err) { addLog(`[${companyName}] DB save scoring error: ${err.message}`); }
      }
    } catch (err) {
      addLog(`[${companyName}] Error: ${err.message}`);
      updateCompany({ status: 'error', step: '', error: err.message });
      // Persist error to Supabase
      if (supabase && company.dbCompanyId) {
        try {
          const rid = company.dbRunId;
          if (rid) await updateResearchRun(rid, { status: 'error', error: err.message });
        } catch (e) { /* ignore */ }
      }
    }
  };

  const processAll = async () => {
    console.log('[processAll] called, companies:', companies.length, 'isProcessing:', isProcessing);
    if (companies.length === 0) { setError('Upload a CSV first'); return; }
    setIsProcessing(true);
    stopRef.current = false;
    setError('');
    const pending = companies.map((_, i) => i).filter(i => companies[i].status === 'pending' || companies[i].status === 'error');
    console.log('[processAll] pending indices:', pending);
    addLog(`Starting Exa+Claude screening of ${pending.length} companies (concurrency: ${concurrency})`);
    let active = 0;
    let nextIdx = 0;
    await new Promise((resolve) => {
      const tryNext = () => {
        if (stopRef.current) { if (active === 0) resolve(); return; }
        while (active < concurrency && nextIdx < pending.length) {
          const idx = pending[nextIdx++];
          active++;
          processCompany(idx).finally(() => { active--; tryNext(); });
        }
        if (active === 0 && nextIdx >= pending.length) resolve();
      };
      tryNext();
    });
    setIsProcessing(false);
    addLog('Screening complete.');
  };

  const stopProcessing = () => { stopRef.current = true; addLog('Stopping...'); };

  // Auto-start processing when companies are added from discovery
  useEffect(() => {
    if (autoStartRef.current && !isProcessing && companies.some(c => c.status === 'pending')) {
      autoStartRef.current = false;
      processAll();
    }
  }, [companies, isProcessing]);

  const downloadCSV = () => {
    const headers = [
      'Company Name', 'Website', 'Total Score', 'ICP Fit',
      'A: Differentiation', 'A: Verdict',
      'B: Outcomes', 'B: Verdict',
      'C: Customer-Centric', 'C: Verdict',
      'D: Product Change', 'D: Verdict',
      'E: Audience Change', 'E: Verdict',
      'F: Multi-Product', 'F: Verdict',
      'Score Summary', 'Disqualified Reason',
      'Manual Score', 'Status'
    ];
    const fmt = (s) => (s || '').replace(/"/g, '""');
    const rows = companies.map(c => [
      `"${fmt(c.companyName)}"`, `"${fmt(c.website)}"`,
      `"${c.totalScore || 0}"`, `"${fmt(c.icpFit)}"`,
      `"${c.scoreA || 0}"`, `"${fmt(c.aVerdict || '')}"`,
      `"${c.scoreB || 0}"`, `"${fmt(c.bVerdict || '')}"`,
      `"${c.scoreC || 0}"`, `"${fmt(c.cVerdict || '')}"`,
      `"${c.scoreD || 0}"`, `"${fmt(c.dVerdict || '')}"`,
      `"${c.scoreE || 0}"`, `"${fmt(c.eVerdict || '')}"`,
      `"${c.scoreF || 0}"`, `"${fmt(c.fVerdict || '')}"`,
      `"${fmt(c.scoreSummary)}"`, `"${fmt(c.disqualificationReason)}"`,
      `"${fmt(c.manualScore)}"`, `"${c.status}"`
    ]);
    const csv = [headers.join(','), ...rows.map(r => r.join(','))].join('\n');
    setCsvContent(csv);
    setShowCsvModal(true);
    try {
      const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = 'icp_screening_results.csv';
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
    } catch (e) { console.log('Download failed, modal shown'); }
  };

  // ======= DISCOVERY FUNCTIONS =======
  const buildCrustdataFilters = () => {
    const conditions = [];
    
    for (const f of activeFilters) {
      const spec = FILTER_CATALOG.find(s => s.key === f.fieldKey);
      if (!spec) continue;
      
      // Skip empty values
      if (Array.isArray(f.value)) {
        if (f.value.length === 0) continue;
      } else if (f.value === '' || f.value === null || f.value === undefined) {
        continue;
      }
      
      // Build the condition
      let value = f.value;
      if (spec.inputType === 'number') value = parseFloat(value);
      if (spec.inputType === 'text_list' && typeof value === 'string') value = value.split(',').map(v => v.trim()).filter(Boolean);
      
      // Special handling: employee_count_range → convert to numeric employee_metrics.latest_count conditions
      if (f.fieldKey === 'employee_count_range' && Array.isArray(value) && value.length > 0) {
        const rangeMap = { '1-10': [1,10], '11-50': [11,50], '51-200': [51,200], '201-500': [201,500], '501-1000': [501,1000], '1001-5000': [1001,5000], '5001-10000': [5001,10000], '10001+': [10001, null] };
        let globalMin = Infinity, globalMax = -Infinity;
        for (const r of value) {
          const [lo, hi] = rangeMap[r] || [null, null];
          if (lo !== null && lo < globalMin) globalMin = lo;
          if (hi === null) globalMax = null; // 10001+ means no upper bound
          else if (globalMax !== null && hi > globalMax) globalMax = hi;
        }
        if (globalMin !== Infinity) conditions.push({ filter_type: 'employee_metrics.latest_count', type: '=>', value: globalMin });
        if (globalMax !== null && globalMax !== -Infinity) conditions.push({ filter_type: 'employee_metrics.latest_count', type: '=<', value: globalMax });
        continue;
      }
      
      // Safety: if value is array but operator expects scalar, auto-convert
      let operator = f.operator;
      if (Array.isArray(value) && (operator === '=' || operator === '!=')) {
        if (value.length === 1) {
          value = value[0]; // single item array → scalar
        } else {
          operator = operator === '=' ? 'in' : 'not_in'; // multi-item array → in/not_in
        }
      }
      
      conditions.push({ filter_type: f.fieldKey, type: operator, value });
    }
    
    // Always exclude companies already in the database
    const existingDomains = companies.filter(c => c.domain).map(c => c.domain);
    if (existingDomains.length > 0) {
      conditions.push({ filter_type: 'company_website_domain', type: 'not_in', value: existingDomains });
    }
    
    if (conditions.length === 0) return null;
    const built = { op: 'and', conditions };
    addLog(`Filters built: ${JSON.stringify(built).substring(0, 500)}`);
    return built;
  };
  
  const runDiscovery = async (useCursor = false) => {
    setDiscoverLoading(true);
    setDiscoverError('');
    if (!useCursor) {
      setDiscoverResults([]);
      setDiscoverCursor(null);
      setDiscoverPage(1);
      setDiscoverTotal(0);
      setDiscoverSelected(new Set());
    }
    
    try {
      if (discoverMode === 'indb') {
        // === In-DB CompanyDB Search ===
        const filters = buildCrustdataFilters();
        if (!filters) {
          setDiscoverError('Add at least one filter.');
          setDiscoverLoading(false);
          return;
        }
        
        const apiSort = DISCOVER_API_SORTS.find(s => s.key === discoverApiSort);
        const sorts = apiSort?.column ? [{ column: apiSort.column, order: apiSort.order }] : [];
        
        const resp = await fetch('/api/crustdata', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            action: 'search',
            filters,
            sorts,
            limit: 25,
            ...(useCursor && discoverCursor ? { cursor: discoverCursor } : {}),
          }),
        });
        
        const data = await resp.json();
        if (data.error) throw new Error(data.error);
        
        const rawCompanies = data.companies || [];
        if (rawCompanies.length > 0) {
          addLog(`Discovery raw field sample: ${Object.keys(rawCompanies[0]).slice(0, 15).join(', ')}`);
        }
        
        const newCompanies = rawCompanies.map(co => {
          const rawDomain = co.website_domain || co.company_website_domain || '';
          const domain = rawDomain.replace(/^www\./, '').replace(/\/$/, '');
          const rawWebsite = co.website || '';
          const website = domain ? `https://${domain}` : rawWebsite;
          const joinArr = (v) => Array.isArray(v) ? v.join(', ') : (v || '');
          const revLo = co.estimated_revenue_lower_bound_usd || 0;
          const revHi = co.estimated_revenue_higher_bound_usd || 0;
          const fmtRev = (v) => v >= 1e9 ? `$${(v/1e9).toFixed(1)}B` : v >= 1e6 ? `$${(v/1e6).toFixed(0)}M` : v >= 1e3 ? `$${(v/1e3).toFixed(0)}K` : v ? `$${v}` : '';
          
          return {
            name: co.company_name || co.name || '',
            domain,
            website,
            industry: joinArr(co.linkedin_industries) || co.industry || '',
            employees: co.employee_metrics?.latest_count || co.employee_count || 0,
            employeeRange: co.employee_count_range || '',
            funding: co.last_funding_round_type || '',
            lastFundingDate: co.last_funding_date || '',
            totalFunding: co.crunchbase_total_investment_usd || 0,
            investors: joinArr(co.crunchbase_investors) || joinArr(co.tracxn_investors),
            categories: joinArr(co.crunchbase_categories) || joinArr(co.categories),
            markets: joinArr(co.markets) || '',
            description: co.linkedin_company_description || co.company_description || '',
            location: co.hq_location || co.location || '',
            hqCountry: co.hq_country || '',
            largestHcCountry: co.largest_headcount_country || '',
            yearFounded: co.year_founded || 0,
            hcEngineering: co.department_metrics?.engineering?.latest_count || 0,
            hcSales: co.department_metrics?.sales?.latest_count || 0,
            hcMarketing: co.department_metrics?.marketing?.latest_count || 0,
            hcOperations: co.department_metrics?.operations?.latest_count || 0,
            hcHR: co.department_metrics?.human_resource?.latest_count || 0,
            growth6m: co.employee_metrics?.growth_6m_percent || 0,
            growth12m: co.employee_metrics?.growth_12m_percent || 0,
            growth12mAbs: co.employee_metrics?.growth_12m || 0,
            engGrowth6m: co.department_metrics?.engineering?.growth_6m_percent || 0,
            salesGrowth6m: co.department_metrics?.sales?.growth_6m_percent || 0,
            mktgGrowth6m: co.department_metrics?.marketing?.growth_6m_percent || 0,
            followers: co.follower_metrics?.latest_count || 0,
            followerGrowth6m: co.follower_metrics?.growth_6m_percent || 0,
            monthlyVisitors: co.monthly_visitors || 0,
            jobOpenings: co.job_openings_count || 0,
            overallRating: co.overall_rating || 0,
            revenueRange: revLo || revHi ? `${fmtRev(revLo)}–${fmtRev(revHi)}` : '',
            acquisitionStatus: co.acquisition_status || '',
            companyType: co.company_type || '',
            ipoDate: co.ipo_date || '',
          };
        }).filter(c => c.domain);
        
        const existingDomains = new Set(companies.map(c => c.domain));
        const filtered = newCompanies.filter(c => !existingDomains.has(c.domain));
        
        setDiscoverResults(prev => useCursor ? [...prev, ...filtered] : filtered);
        setDiscoverCursor(data.next_cursor || null);
        setDiscoverTotal(data.total_count || data.total_display_count || 0);
        addLog(`Discovery (In-DB): ${filtered.length} new companies (${data.total_count || 0} total)`);
      } else {
        // === LinkedIn Company Search ===
        const filters = buildLinkedInFilters();
        if (filters.length === 0) {
          setDiscoverError('Add at least one filter.');
          setDiscoverLoading(false);
          return;
        }
        
        const page = useCursor ? discoverPage + 1 : 1;
        const resp = await fetch('/api/crustdata', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            action: 'linkedin_company_search',
            filters,
            page,
            limit: 25,
          }),
        });
        
        const data = await resp.json();
        if (data.error) throw new Error(data.error);
        
        const rawCompanies = data.companies || data.profiles || [];
        if (rawCompanies.length > 0) {
          addLog(`LinkedIn discovery raw field sample: ${Object.keys(rawCompanies[0]).slice(0, 15).join(', ')}`);
        }
        
        const newCompanies = rawCompanies.map(co => {
          const rawDomain = co.website_domain || co.company_website_domain || co.website || '';
          const domain = rawDomain.replace(/^https?:\/\//, '').replace(/^www\./, '').replace(/\/$/, '');
          const website = domain ? `https://${domain}` : '';
          const industry = Array.isArray(co.linkedin_industries) ? co.linkedin_industries.join(', ')
            : (co.industry || co.linkedin_industry || '');
          
          return {
            name: co.company_name || co.name || '',
            domain,
            website,
            industry,
            employees: co.employee_metrics?.latest_count || co.company_headcount || co.employee_count || 0,
            funding: co.last_funding_round_type || '',
            totalFunding: co.crunchbase_total_investment_usd || 0,
            investors: Array.isArray(co.crunchbase_investors) ? co.crunchbase_investors.join(', ')
              : Array.isArray(co.tracxn_investors) ? co.tracxn_investors.join(', ')
              : (co.investors || ''),
            categories: Array.isArray(co.crunchbase_categories) ? co.crunchbase_categories.join(', ')
              : Array.isArray(co.categories) ? co.categories.join(', ') : (co.categories || ''),
            description: co.linkedin_company_description || co.company_description || '',
            location: co.hq_location || co.location || co.hq || '',
            yearFounded: co.year_founded || 0,
            hcEngineering: co.department_metrics?.engineering?.latest_count || 0,
            hcSales: co.department_metrics?.sales?.latest_count || 0,
            hcMarketing: co.department_metrics?.marketing?.latest_count || 0,
            hcOperations: co.department_metrics?.operations?.latest_count || 0,
            hcHR: co.department_metrics?.human_resource?.latest_count || 0,
            growth6m: co.employee_metrics?.growth_6m_percent || 0,
            growth12m: co.employee_metrics?.growth_12m_percent || 0,
            followers: co.follower_metrics?.latest_count || 0,
            monthlyVisitors: co.monthly_visitors || 0,
            jobOpenings: co.job_openings_count || 0,
            overallRating: co.overall_rating || 0,
            revenueRange: '',
            acquisitionStatus: co.acquisition_status || '',
            companyType: co.company_type || '',
          };
        }).filter(c => c.domain);
        
        const existingDomains = new Set(companies.map(c => c.domain));
        const filtered = newCompanies.filter(c => !existingDomains.has(c.domain));
        
        setDiscoverResults(prev => useCursor ? [...prev, ...filtered] : filtered);
        setDiscoverPage(page);
        setDiscoverTotal(data.total_count || data.total_display_count || 0);
        addLog(`Discovery (LinkedIn): ${filtered.length} new companies (${data.total_count || 0} total), page ${page}`);
      }
    } catch (err) {
      setDiscoverError(err.message);
      addLog(`Discovery error: ${err.message}`);
    }
    setDiscoverLoading(false);
  };
  
  const toggleDiscoverSelect = (domain) => {
    setDiscoverSelected(prev => {
      const next = new Set(prev);
      if (next.has(domain)) next.delete(domain);
      else next.add(domain);
      return next;
    });
  };
  
  const selectAllDiscovered = () => {
    if (discoverSelected.size === discoverResults.length) {
      setDiscoverSelected(new Set());
    } else {
      setDiscoverSelected(new Set(discoverResults.map(c => c.domain)));
    }
  };
  
  // Move company from screened to active (Add to Database)
  const addToDatabase = async (domain) => {
    setCompanies(prev => prev.map(c => c.domain === domain ? { ...c, dbStatus: 'active' } : c));
    const company = companies.find(c => c.domain === domain);
    if (company?.dbCompanyId) {
      try { await dbUpdateCompany(company.dbCompanyId, { database_status: 'active' }); }
      catch (err) { addLog(`Error updating status: ${err.message}`); }
    }
    addLog(`Added ${domain} to database`);
  };

  const bulkAddToDatabase = async (domains) => {
    setCompanies(prev => prev.map(c => domains.has(c.domain) ? { ...c, dbStatus: 'active' } : c));
    for (const domain of domains) {
      const company = companies.find(c => c.domain === domain);
      if (company?.dbCompanyId) {
        try { await dbUpdateCompany(company.dbCompanyId, { database_status: 'active' }); }
        catch (err) { /* continue */ }
      }
    }
    addLog(`Added ${domains.size} companies to database`);
  };

  const dismissCompany = async (domain) => {
    setCompanies(prev => prev.map(c => c.domain === domain ? { ...c, dbStatus: 'dismissed' } : c));
    const company = companies.find(c => c.domain === domain);
    if (company?.dbCompanyId) {
      try { await dbUpdateCompany(company.dbCompanyId, { database_status: 'dismissed' }); }
      catch (err) { addLog(`Error dismissing: ${err.message}`); }
    }
    addLog(`Dismissed ${domain}`);
  };

  const bulkDismiss = async (domains) => {
    setCompanies(prev => prev.map(c => domains.has(c.domain) ? { ...c, dbStatus: 'dismissed' } : c));
    for (const domain of domains) {
      const company = companies.find(c => c.domain === domain);
      if (company?.dbCompanyId) {
        try { await dbUpdateCompany(company.dbCompanyId, { database_status: 'dismissed' }); }
        catch (err) { /* continue */ }
      }
    }
    addLog(`Dismissed ${domains.size} companies`);
  };

  const addDiscoveredToQueue = (selected = null) => {
    const toAdd = selected ? discoverResults.filter(c => selected.has(c.domain)) : discoverResults;
    const existingDomains = new Set(companies.map(c => c.domain));
    const newCompanies = toAdd
      .filter(c => !existingDomains.has(c.domain))
      .map(c => ({
        companyName: c.name,
        website: c.website,
        domain: c.domain,
        dbStatus: 'screened',
        status: 'pending',
        step: '',
        error: null,
        funding: c.totalFunding ? `$${(c.totalFunding / 1e6).toFixed(1)}M total (${c.funding})` : c.funding,
        teamSize: c.employees ? `~${c.employees} employees` : '',
      }));
    
    if (newCompanies.length > 0) {
      setCompanies(prev => [...prev, ...newCompanies]);
      addLog(`Added ${newCompanies.length} companies from discovery to screening queue`);
      setDiscoverSelected(new Set());
      setActiveView('upload');
      autoStartRef.current = true;
    }
  };

  // ======= CONTACTS SEARCH FUNCTIONS =======
  // Title autocomplete via Crustdata PersonDB Autocomplete API
  const titleAutocompleteTimer = useRef(null);
  // Shared parser for Crustdata person profiles
  const parsePersonProfile = (p) => {
    const currentJob = (p.current_employers || [])[0] || {};
    const edu = (p.education_background || [])[0];
    return {
      id: p.person_id,
      name: p.name || '',
      headline: p.headline || '',
      summary: p.summary || '',
      title: currentJob.title || '',
      company: currentJob.name || '',
      companyDomain: currentJob.company_website_domain || '',
      seniority: currentJob.seniority_level || '',
      function: currentJob.function_category || '',
      linkedin: p.linkedin_profile_url || '',
      region: p.region || '',
      experience: p.years_of_experience_raw || 0,
      connections: p.num_of_connections || 0,
      emailVerified: currentJob.business_email_verified || false,
      recentJobChange: p.recently_changed_jobs || false,
      jobStartDate: currentJob.start_date || '',
      jobLocation: currentJob.location || '',
      skills: (p.skills || []).slice(0, 10).join(', '),
      education: edu ? `${edu.degree_name || ''} ${edu.field_of_study ? '- ' + edu.field_of_study : ''} @ ${edu.institute_name || ''}`.trim() : '',
      allTitles: (p.all_titles || []).slice(0, 5).join(', '),
    };
  };

  const fetchTitleSuggestions = (query) => {
    if (titleAutocompleteTimer.current) clearTimeout(titleAutocompleteTimer.current);
    if (!query || query.length < 2) { setTitleSuggestions([]); return; }
    titleAutocompleteTimer.current = setTimeout(async () => {
      setTitleAutocompleteLoading(true);
      try {
        const resp = await fetch('/api/crustdata', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ action: 'people_autocomplete', field: 'current_employers.title', query, limit: 8 }),
        });
        const data = await resp.json();
        console.log('[titleAutocomplete] query:', query, 'results:', data);
        // Response is array of {value, count} or similar
        const suggestions = Array.isArray(data) ? data.map(d => typeof d === 'string' ? d : d.value || d.label || '') : [];
        setTitleSuggestions(suggestions.filter(Boolean));
      } catch (err) {
        console.error('[titleAutocomplete] error:', err);
        setTitleSuggestions([]);
      }
      setTitleAutocompleteLoading(false);
    }, 300);
  };

  // Preset contact search batches — run automatically when "Find Contacts" is clicked
  const CONTACT_SEARCH_PRESETS = [
    { label: 'Director+ in Marketing & Sales', functions: ['Marketing', 'Sales'], titles: ['Director', 'VP', 'Head of', 'Senior Director', 'SVP', 'EVP', 'CMO', 'CRO', 'Chief Marketing', 'Chief Revenue'] },
    { label: 'Founders', functions: [], titles: ['Co-Founder', 'Founder'] },
    { label: 'CEO / COO', functions: [], titles: ['CEO', 'COO', 'Chief Executive', 'Chief Operating'] },
  ];

  const openContactsModal = (company = null) => {
    setContactsCompany(company);
    setContactsResults([]);
    setContactsError('');
    setContactsCursor(null);
    setContactsTotal(0);
    setSelectedContactResults(new Set());
    setContactsFilters({ titles: '', functions: [], verifiedEmailOnly: false, recentlyChangedJobs: false });
    setTitleSuggestions([]);
    setPeopleFilters([]);
    setPeopleAutocompleteResults({});
    setActiveView('discover_contacts');
  };

  const buildPresetFilters = (company) => {
    if (!company) return null;
    const conditions = [];

    // Company domain filter
    if (Array.isArray(company)) {
      const domains = company.map(c => c.domain).filter(Boolean);
      if (domains.length === 0) return null;
      if (domains.length === 1) {
        conditions.push({ column: 'current_employers.company_website_domain', type: '=', value: domains[0] });
      } else {
        conditions.push({ column: 'current_employers.company_website_domain', type: 'in', value: domains });
      }
    } else {
      if (!company.domain) return null;
      conditions.push({ column: 'current_employers.company_website_domain', type: '=', value: company.domain });
    }

    // Build OR group across all presets
    const presetConditions = CONTACT_SEARCH_PRESETS.map(preset => {
      const titleOr = {
        op: 'or',
        conditions: preset.titles.map(t => ({ column: 'current_employers.title', type: '(.)', value: t }))
      };
      if (preset.functions.length > 0) {
        // This preset requires BOTH function match AND title match
        return {
          op: 'and',
          conditions: [
            { column: 'current_employers.function_category', type: 'in', value: preset.functions },
            titleOr,
          ]
        };
      }
      // No function filter — just title match
      return titleOr;
    });

    conditions.push({ op: 'or', conditions: presetConditions });

    return { op: 'and', conditions };
  };

  const runPresetContactSearch = async (company) => {
    if (!company) return;
    setContactsLoading(true);
    setContactsError('');
    setContactsResults([]);
    setSelectedContactResults(new Set());

    try {
      const filters = buildPresetFilters(company);
      if (!filters) { setContactsError('No company selected.'); setContactsLoading(false); return; }

      addLog(`Contact search (single call): ${JSON.stringify(filters).substring(0, 500)}`);

      const resp = await fetch('/api/crustdata', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: 'people_search', filters, limit: 50 }),
      });
      const data = await resp.json();
      if (data.error) throw new Error(data.error);

      const people = (data.profiles || []).map(parsePersonProfile);

      setContactsResults(people);
      setContactsTotal(data.total_count || people.length);
      setSelectedContactResults(new Set(people.map(p => p.id || p.linkedin)));
      setContactsCursor(data.next_cursor || null);
      addLog(`Contact search: ${people.length} results (${data.total_count || 0} total)`);
    } catch (err) {
      setContactsError(err.message);
      addLog(`Contact search error: ${err.message}`);
    }
    setContactsLoading(false);
  };

  // Batch find contacts: pass all checked accounts as a multi-domain filter
  const batchFindContacts = () => {
    if (checkedAccounts.size === 0) return;
    const checkedCompanies = databaseAccounts.filter(c => checkedAccounts.has(c.domain));
    if (checkedCompanies.length === 0) return;
    if (checkedCompanies.length === 1) {
      openContactsModal({ domain: checkedCompanies[0].domain, name: checkedCompanies[0].companyName });
    } else {
      // Pass array of companies for multi-domain search
      openContactsModal(checkedCompanies.map(c => ({ domain: c.domain, name: c.companyName })));
    }
  };
  
  // Build filters for Crustdata In-DB People Search API (/screener/persondb/search)
  // Docs: uses {column, type, value} format with op: "and"/"or" for combining
  // Operators: "=" (case-insensitive exact), "in" (case-sensitive list), "(.) " (fuzzy text), "[.]" (substring)
  const buildPeopleFilters = () => {
    const conditions = [];
    
    // Company filter — contactsCompany can be:
    //   {domain, name} — single company, use "=" (case-insensitive, fast)
    //   [{domain, name}, ...] — multi company, use "in" (case-sensitive)
    //   null — no company selected, reject
    if (!contactsCompany) return null;
    
    if (Array.isArray(contactsCompany)) {
      const domains = contactsCompany.map(c => c.domain).filter(Boolean);
      if (domains.length === 0) return null;
      if (domains.length === 1) {
        conditions.push({ column: 'current_employers.company_website_domain', type: '=', value: domains[0] });
      } else {
        conditions.push({ column: 'current_employers.company_website_domain', type: 'in', value: domains });
      }
    } else {
      if (!contactsCompany.domain) return null;
      conditions.push({ column: 'current_employers.company_website_domain', type: '=', value: contactsCompany.domain });
    }
    
    // Title filter — use "(.) " (fuzzy) for flexible matching, supports typos and variations
    // Per docs: multi-word fuzzy searches each word independently (all must be present)
    // For multiple titles, use OR across separate fuzzy conditions (docs: "(.) does not support OR in value string")
    if (contactsFilters.titles.trim()) {
      const titles = contactsFilters.titles.split(',').map(t => t.trim()).filter(Boolean);
      if (titles.length === 1) {
        conditions.push({ column: 'current_employers.title', type: '(.)', value: titles[0] });
      } else {
        conditions.push({
          op: 'or',
          conditions: titles.map(t => ({ column: 'current_employers.title', type: '(.)', value: t }))
        });
      }
    }
    
    // Function category — "in" with array
    if (contactsFilters.functions.length > 0) {
      conditions.push({ column: 'current_employers.function_category', type: 'in', value: contactsFilters.functions });
    }
    
    // Verified business email — boolean with "="
    if (contactsFilters.verifiedEmailOnly) {
      conditions.push({ column: 'current_employers.business_email_verified', type: '=', value: true });
    }
    
    // Recently changed jobs — boolean with "="
    if (contactsFilters.recentlyChangedJobs) {
      conditions.push({ column: 'recently_changed_jobs', type: '=', value: true });
    }
    
    if (conditions.length === 0) return null;
    // Always wrap in op:and — single condition passthrough may cause API errors
    return { op: 'and', conditions };
  };
  
  const searchContacts = async (useCursor = false) => {
    const t0 = performance.now();
    const companyLabel = Array.isArray(contactsCompany)
      ? `${contactsCompany.length} accounts (${contactsCompany.map(c => c.domain).join(', ')})`
      : contactsCompany?.domain || 'unknown';
    console.log('[searchContacts] START, useCursor:', useCursor, 'company:', companyLabel);
    addLog(`Contact search started for ${Array.isArray(contactsCompany) ? `${contactsCompany.length} accounts` : contactsCompany?.name || 'unknown'}...`);
    setContactsLoading(true);
    setContactsError('');
    if (!useCursor) {
      setContactsResults([]);
      setContactsCursor(null);
      setContactsTotal(0);
      setSelectedContactResults(new Set());
    }
    
    try {
      const t1 = performance.now();
      const filters = buildPeopleFilters();
      console.log('[searchContacts] buildFilters took', (performance.now() - t1).toFixed(0), 'ms');
      console.log('[searchContacts] filters:', JSON.stringify(filters));
      if (!filters) {
        const errMsg = contactsCompany ? 'Unable to build filter.' : 'Please select a specific account to search contacts for.';
        console.log('[searchContacts] no filters:', errMsg);
        setContactsError(errMsg);
        setContactsLoading(false);
        return;
      }
      
      const requestBody = {
        action: 'people_search',
        filters,
        limit: 50,
        ...(useCursor && contactsCursor ? { cursor: contactsCursor } : {}),
      };
      console.log('[searchContacts] request body:', JSON.stringify(requestBody).substring(0, 1000));
      
      const t2 = performance.now();
      addLog(`Sending Crustdata API request...`);
      const resp = await fetch('/api/crustdata', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestBody),
      });
      const fetchMs = (performance.now() - t2).toFixed(0);
      console.log('[searchContacts] fetch completed in', fetchMs, 'ms, status:', resp.status);
      addLog(`API responded in ${fetchMs}ms (status ${resp.status})`);
      
      const t3 = performance.now();
      const data = await resp.json();
      const parseMs = (performance.now() - t3).toFixed(0);
      console.log('[searchContacts] JSON parse took', parseMs, 'ms, keys:', Object.keys(data), 'profiles:', data.profiles?.length);
      if (data.error) throw new Error(data.error);
      
      const t4 = performance.now();
      const people = (data.profiles || []).map(parsePersonProfile);
      const mapMs = (performance.now() - t4).toFixed(0);
      console.log('[searchContacts] mapped', people.length, 'people in', mapMs, 'ms');
      
      setContactsResults(prev => useCursor ? [...prev, ...people] : people);
      setContactsCursor(data.next_cursor || null);
      setContactsTotal(data.total_count || 0);
      addLog(`Contacts search: ${people.length} results (${data.total_count || 0} total) — API ${fetchMs}ms`);
    } catch (err) {
      setContactsError(err.message);
      addLog(`Contacts search error: ${err.message}`);
      console.error('[searchContacts] error:', err);
    }
    setContactsLoading(false);
    const totalMs = (performance.now() - t0).toFixed(0);
    console.log('[searchContacts] DONE total time:', totalMs, 'ms');
    addLog(`Contact search completed in ${totalMs}ms total`);
  };
  
  // Add selected (or all) contacts from search results to DB, then enrich emails
  const [addingContacts, setAddingContacts] = useState(false);
  const addContactsAndEnrich = async () => {
    if (selectedContactResults.size === 0) return;
    const toAdd = contactsResults.filter(p => selectedContactResults.has(p.id || p.linkedin));

    if (toAdd.length === 0) return;
    setAddingContacts(true);

    try {
      // Build domain → company_id map
      const domainToCompanyId = {};
      companies.forEach(c => { if (c.dbCompanyId && c.domain) domainToCompanyId[c.domain] = c.dbCompanyId; });

      const dbContacts = toAdd.filter(p => p.linkedin && domainToCompanyId[p.companyDomain]).map(p => ({
        company_id: domainToCompanyId[p.companyDomain],
        name: p.name,
        title: p.title,
        linkedin: p.linkedin,
        email_verified: p.emailVerified,
        seniority: p.seniority,
        function_category: p.function,
        region: p.region,
        headline: p.headline,
        years_experience: p.experience,
        recent_job_change: p.recentJobChange,
        company_domain: p.companyDomain,
        crustdata_person_id: p.id || null,
      }));

      if (dbContacts.length > 0 && supabase) {
        addLog(`Adding ${dbContacts.length} contacts to database...`);
        await upsertContacts(dbContacts);
        const refreshed = await getAllContacts();
        setAllContacts(refreshed);
        addLog(`Saved ${dbContacts.length} contacts. Starting email enrichment...`);

        // Now enrich contacts that don't have emails
        const contactsToEnrich = refreshed.filter(ct =>
          !ct.business_email && ct.linkedin &&
          dbContacts.some(dc => dc.linkedin === ct.linkedin)
        );

        if (contactsToEnrich.length > 0) {
          addLog(`Enriching ${contactsToEnrich.length} contacts for email...`);
          for (const ct of contactsToEnrich) {
            await enrichContact(ct);
            await new Promise(r => setTimeout(r, 300)); // rate limit
          }
          addLog(`Email enrichment complete.`);
        } else {
          addLog(`All added contacts already have emails or no LinkedIn URLs.`);
        }
      }

      // Switch to contacts tab after adding
      setActiveView('contacts');
      setContactsResults([]);
      setSelectedContactResults(new Set());
    } catch (err) {
      addLog(`Error adding contacts: ${err.message}`);
    }
    setAddingContacts(false);
  };

  // Enrich a contact via Crustdata Person Enrich API to get business email
  const enrichContact = async (contact) => {
    if (!contact.linkedin) {
      addLog(`Cannot enrich ${contact.name}: no LinkedIn URL`);
      return;
    }
    
    setEnrichingContacts(prev => ({ ...prev, [contact.id]: 'loading' }));
    addLog(`Enriching ${contact.name}...`);
    
    try {
      // Per Crustdata docs: fields=business_email returns ONLY email (2 credits)
      // enrich_realtime=true fetches from web if not in DB (5 credits + 2 for email = 7 total)
      const resp = await fetch('/api/crustdata', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          action: 'person_enrich',
          linkedin_profile_url: contact.linkedin,
          fields: 'business_email',
          enrich_realtime: true,
        }),
      });
      
      const data = await resp.json();
      console.log('[enrichContact] response:', JSON.stringify(data).substring(0, 500));
      
      // Check for error (including 404 PE03 "profile not found, queued")
      if (data.error) throw new Error(data.error);
      
      // Response is an array of profiles per docs
      const profiles = Array.isArray(data) ? data : [data];
      const profile = profiles[0];
      
      if (!profile) throw new Error('No profile returned');
      
      // Extract business email per docs response format for fields=business_email:
      // { "business_email": ["chris@crustdata.com"], "current_employers": [{ "business_emails": { "chris@crustdata.com": { "verification_status": "verified" } } }] }
      let email = '';
      
      // Path 1: top-level business_email array
      if (profile.business_email) {
        const emailVal = Array.isArray(profile.business_email) ? profile.business_email[0] : profile.business_email;
        if (emailVal && typeof emailVal === 'string') email = emailVal;
      }
      
      // Path 2: current_employers[].business_emails object keys
      if (!email && profile.current_employers) {
        for (const emp of profile.current_employers) {
          if (emp.business_emails && typeof emp.business_emails === 'object') {
            const emailKeys = Object.keys(emp.business_emails);
            if (emailKeys.length > 0) { email = emailKeys[0]; break; }
          }
        }
      }
      
      // Path 3: also check past_employers for business_emails
      if (!email && profile.past_employers) {
        for (const emp of profile.past_employers) {
          if (emp.business_emails && typeof emp.business_emails === 'object') {
            const emailKeys = Object.keys(emp.business_emails);
            if (emailKeys.length > 0) { email = emailKeys[0]; break; }
          }
        }
      }
      
      if (email) {
        addLog(`Found email for ${contact.name}: ${email}`);
        
        // Update in DB — strip joined relation data before upserting
        if (supabase) {
          const { companies, ...contactFields } = contact;
          await upsertContact({
            ...contactFields,
            business_email: email,
            last_enriched_at: new Date().toISOString(),
          });
          // Refresh contacts
          const refreshed = await getAllContacts();
          setAllContacts(refreshed);
        }
        
        setEnrichingContacts(prev => ({ ...prev, [contact.id]: 'done' }));
      } else {
        addLog(`No email found for ${contact.name}`);
        setEnrichingContacts(prev => ({ ...prev, [contact.id]: 'error' }));
      }
    } catch (err) {
      addLog(`Enrich error for ${contact.name}: ${err.message}`);
      setEnrichingContacts(prev => ({ ...prev, [contact.id]: 'error' }));
    }
  };

  // Bulk enrich: enrich all selected contacts (or all contacts for an account)
  const enrichContactsBulk = async (contactsList) => {
    for (const ct of contactsList) {
      if (ct.business_email) continue; // skip already enriched
      if (!ct.linkedin) continue;
      await enrichContact(ct);
      // Small delay to avoid rate limits
      await new Promise(r => setTimeout(r, 500));
    }
  };

  // ======= CAMPAIGN FUNCTIONS =======
  const loadCampaignData = async (campaignId) => {
    if (!campaignId) { setCampaignContacts([]); setCampaignMessages([]); setCampaignPrompt(''); setContactGenMessages([]); setGenContactsWithMessages(new Set()); return; }
    try {
      const [cts, msgs] = await Promise.all([
        getCampaignContacts(campaignId),
        getCampaignMessages(campaignId),
      ]);
      setCampaignContacts(cts);
      setCampaignMessages(msgs);
      // Load prompt from campaign
      const camp = campaigns.find(c => c.id === campaignId);
      setCampaignPrompt(camp?.prompt || '');
      // Load which contacts have generated messages
      const genContactIds = await getGeneratedMessageContacts(campaignId);
      setGenContactsWithMessages(new Set(genContactIds));
    } catch (err) {
      addLog(`Campaign load error: ${err.message}`);
    }
  };

  useEffect(() => {
    if (activeCampaignId) loadCampaignData(activeCampaignId);
  }, [activeCampaignId]);

  // Load generated messages when selecting a contact in campaign view
  const loadContactGeneratedMessages = async (contactId) => {
    if (!activeCampaignId || !contactId) { setContactGenMessages([]); return; }
    try {
      const msgs = await getGeneratedMessages(activeCampaignId, contactId);
      setContactGenMessages(msgs);
    } catch (err) {
      addLog(`Load generated messages error: ${err.message}`);
      setContactGenMessages([]);
    }
  };

  // When selected campaign contact changes, load their generated messages
  useEffect(() => {
    if (selectedCampaignContact && activeCampaignId) {
      loadContactGeneratedMessages(selectedCampaignContact);
    } else {
      setContactGenMessages([]);
    }
    setContactDraftMessages([]);
    setContactReviewNotes('');
    setContactGapSelection(null);
  }, [selectedCampaignContact, activeCampaignId]);

  const handleCreateCampaign = async () => {
    const name = prompt('Campaign name:');
    if (!name) return;
    try {
      const camp = await createCampaign(name);
      setCampaigns(prev => [camp, ...prev]);
      setActiveCampaignId(camp.id);
      addLog(`Created campaign: ${name}`);
    } catch (err) { addLog(`Create campaign error: ${err.message}`); }
  };

  const handleDeleteCampaign = async () => {
    if (!activeCampaignId) return;
    const camp = campaigns.find(c => c.id === activeCampaignId);
    if (!confirm(`Delete campaign "${camp?.name}"?`)) return;
    try {
      await deleteCampaign(activeCampaignId);
      setCampaigns(prev => prev.filter(c => c.id !== activeCampaignId));
      setActiveCampaignId(campaigns.find(c => c.id !== activeCampaignId)?.id || null);
      setCampaignContacts([]);
      setCampaignMessages([]);
    } catch (err) { addLog(`Delete campaign error: ${err.message}`); }
  };

  const handleAddContactsToCampaign = async (contactIds) => {
    if (!activeCampaignId || contactIds.length === 0) return;
    try {
      await addContactsToCampaign(activeCampaignId, contactIds);
      await loadCampaignData(activeCampaignId);
      setAddToCampaignModal(false);
      addLog(`Added ${contactIds.length} contacts to campaign`);
    } catch (err) { addLog(`Add contacts error: ${err.message}`); }
  };

  const handleRemoveContactFromCampaign = async (contactId) => {
    if (!activeCampaignId) return;
    try {
      await removeContactFromCampaign(activeCampaignId, contactId);
      setCampaignContacts(prev => prev.filter(cc => cc.contact_id !== contactId));
      if (selectedCampaignContact === contactId) setSelectedCampaignContact(null);
    } catch (err) { addLog(`Remove contact error: ${err.message}`); }
  };

  // Bulk actions for contacts tab
  const handleBulkDeleteContacts = async () => {
    if (checkedContacts.size === 0) return;
    if (!confirm(`Delete ${checkedContacts.size} contact${checkedContacts.size > 1 ? 's' : ''}? This cannot be undone.`)) return;
    let deleted = 0;
    for (const id of checkedContacts) {
      try {
        await deleteContact(id);
        deleted++;
      } catch (err) { addLog(`Delete contact ${id} error: ${err.message}`); }
    }
    setAllContacts(prev => prev.filter(c => !checkedContacts.has(c.id)));
    if (checkedContacts.has(selectedContactId)) setSelectedContactId(null);
    setCheckedContacts(new Set());
    addLog(`Deleted ${deleted} contacts`);
  };

  const [bulkCampaignPicker, setBulkCampaignPicker] = useState(false);

  // Bulk delete accounts
  const handleBulkDeleteAccounts = async () => {
    if (checkedAccounts.size === 0) return;
    const checkedCompanies = companies.filter(c => checkedAccounts.has(c.domain));
    const contactCount = allContacts.filter(ct => checkedAccounts.has(ct.company_domain)).length;
    const msg = contactCount > 0
      ? `Delete ${checkedCompanies.length} account${checkedCompanies.length > 1 ? 's' : ''} and their ${contactCount} contact${contactCount > 1 ? 's' : ''}? This cannot be undone.`
      : `Delete ${checkedCompanies.length} account${checkedCompanies.length > 1 ? 's' : ''}? This cannot be undone.`;
    if (!confirm(msg)) return;
    let deleted = 0;
    for (const c of checkedCompanies) {
      try {
        if (c.dbCompanyId) {
          await deleteCompany(c.dbCompanyId);
        }
        deleted++;
      } catch (err) { addLog(`Delete account ${c.domain} error: ${err.message}`); }
    }
    // Remove from local state
    setCompanies(prev => prev.filter(c => !checkedAccounts.has(c.domain)));
    setAllContacts(prev => prev.filter(ct => !checkedAccounts.has(ct.company_domain)));
    if (selectedCompany !== null && checkedAccounts.has(companies[selectedCompany]?.domain)) {
      setSelectedCompany(null);
    }
    setCheckedAccounts(new Set());
    addLog(`Deleted ${deleted} accounts${contactCount > 0 ? ` and ${contactCount} contacts` : ''}`);
  };

  // Update account status (single)
  const updateAccountStatus = async (companyIdx, newStatus) => {
    setCompanies(prev => { const u = [...prev]; u[companyIdx] = { ...u[companyIdx], accountStatus: newStatus }; return u; });
    const c = companies[companyIdx];
    if (supabase && c.dbCompanyId) {
      try { await dbUpdateCompany(c.dbCompanyId, { account_status: newStatus }); } catch (err) { addLog(`Status update error: ${err.message}`); }
    }
  };

  // Bulk update account status
  const bulkUpdateAccountStatus = async (newStatus) => {
    if (checkedAccounts.size === 0) return;
    const updates = [];
    setCompanies(prev => prev.map(c => {
      if (checkedAccounts.has(c.domain)) {
        updates.push(c);
        return { ...c, accountStatus: newStatus };
      }
      return c;
    }));
    for (const c of updates) {
      if (supabase && c.dbCompanyId) {
        try { await dbUpdateCompany(c.dbCompanyId, { account_status: newStatus }); } catch (err) { addLog(`Bulk status error for ${c.domain}: ${err.message}`); }
      }
    }
    addLog(`Updated ${updates.length} accounts to "${newStatus}"`);
    setBulkStatusPicker(false);
    setCheckedAccounts(new Set());
  };

  // Export accounts CSV
  const exportAccountsCSV = () => {
    const toExport = checkedAccounts.size > 0
      ? databaseAccounts.filter(c => checkedAccounts.has(c.domain))
      : databaseAccounts;
    if (toExport.length === 0) return;
    const fmt = (s) => (s || '').toString().replace(/"/g, '""');
    const headers = ['Company Name', 'Domain', 'Total Score', 'ICP Fit', 'Status', 'A', 'B', 'C', 'D', 'E', 'F', 'Summary', 'Manual Score', 'Contacts'];
    const rows = toExport.map(c => {
      const contactCount = allContacts.filter(ct => ct.company_domain === c.domain).length;
      return [
        `"${fmt(c.companyName)}"`, `"${fmt(c.domain)}"`, c.totalScore || 0, `"${fmt(c.icpFit)}"`, `"${fmt(c.accountStatus || 'Cold')}"`,
        c.scoreA || 0, c.scoreB || 0, c.scoreC || 0, c.scoreD || 0, c.scoreE || 0, c.scoreF || 0,
        `"${fmt(c.scoreSummary)}"`, `"${fmt(c.manualScore)}"`, contactCount
      ];
    });
    const csv = [headers.join(','), ...rows.map(r => r.join(','))].join('\n');
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = checkedAccounts.size > 0 ? `accounts_${checkedAccounts.size}_selected.csv` : 'accounts_all.csv';
    document.body.appendChild(a); a.click(); document.body.removeChild(a);
  };
  const [findContactsPicker, setFindContactsPicker] = useState(null); // 'accounts' | 'contacts' | null
  const handleBulkAddToCampaign = async (campaignId) => {
    if (checkedContacts.size === 0 || !campaignId) return;
    try {
      await addContactsToCampaign(campaignId, Array.from(checkedContacts));
      const camp = campaigns.find(c => c.id === campaignId);
      addLog(`Added ${checkedContacts.size} contacts to "${camp?.name || 'campaign'}"`);
      setCheckedContacts(new Set());
      setBulkCampaignPicker(false);
      // Refresh contacts to get updated last_campaign_added_at
      const refreshed = await getAllContacts();
      setAllContacts(refreshed);
      // Refresh campaign contact mappings
      const ccMappings = await getAllCampaignContacts();
      setAllCampaignContacts(ccMappings);
      if (activeCampaignId === campaignId) {
        const data = await getCampaignContacts(campaignId);
        setCampaignContacts(data);
      }
    } catch (err) { addLog(`Bulk add to campaign error: ${err.message}`); }
  };

  const updateContactStatus = async (contactId, newStatus) => {
    try {
      await updateContact(contactId, { contact_status: newStatus });
      setAllContacts(prev => prev.map(c => c.id === contactId ? { ...c, contact_status: newStatus } : c));
    } catch (err) { addLog(`Contact status error: ${err.message}`); }
  };

  const handleAddMessage = async (channel = 'email') => {
    if (!activeCampaignId) return;
    const nextStep = campaignMessages.length > 0 ? Math.max(...campaignMessages.map(m => m.step_number)) + 1 : 1;
    try {
      const msg = await createCampaignMessage(activeCampaignId, channel, nextStep);
      setCampaignMessages(prev => [...prev, msg]);
    } catch (err) { addLog(`Add message error: ${err.message}`); }
  };

  const handleSaveMessage = async (msg) => {
    try {
      const updated = await upsertCampaignMessage(msg);
      setCampaignMessages(prev => prev.map(m => m.id === updated.id ? updated : m));
    } catch (err) { addLog(`Save message error: ${err.message}`); }
  };

  const handleDeleteMessage = async (msgId) => {
    try {
      await deleteCampaignMessage(msgId);
      setCampaignMessages(prev => prev.filter(m => m.id !== msgId));
    } catch (err) { addLog(`Delete message error: ${err.message}`); }
  };

  // Save prompt to campaign (debounced)
  const savePromptTimeout = useRef(null);
  const handlePromptChange = (val) => {
    setCampaignPrompt(val);
    if (savePromptTimeout.current) clearTimeout(savePromptTimeout.current);
    savePromptTimeout.current = setTimeout(async () => {
      if (activeCampaignId) {
        try {
          await updateCampaign(activeCampaignId, { prompt: val });
          setCampaigns(prev => prev.map(c => c.id === activeCampaignId ? { ...c, prompt: val } : c));
        } catch (err) { addLog(`Save prompt error: ${err.message}`); }
      }
    }, 1500);
  };

  // Build context for a contact/account pair
  const buildContactContext = (ct, fullAccount) => {
    const fields = {
      // Contact fields
      'contact.name': ct.name || '',
      'contact.title': ct.title || '',
      'contact.business_email': ct.business_email || '',
      'contact.seniority': ct.seniority || '',
      'contact.function': ct.function_category || '',
      'contact.region': ct.region || '',
      'contact.headline': ct.headline || '',
      'contact.linkedin': ct.linkedin || '',
      // Account fields
      'account.companyName': fullAccount?.companyName || ct.companies?.name || '',
      'account.domain': fullAccount?.domain || ct.company_domain || '',
      'account.productSummary': fullAccount?.productSummary || '',
      'account.targetCustomer': fullAccount?.targetCustomer || '',
      'account.targetDecisionMaker': fullAccount?.targetDecisionMaker || '',
      'account.competitors': fullAccount?.competitors || '',
      'account.linkedinDescription': fullAccount?.linkedinDescription || '',
      'account.ceoFounderName': fullAccount?.ceoFounderName || '',
      'account.ceoNarrativeTheme': fullAccount?.ceoNarrativeTheme || '',
      'account.newMarketingLeader': fullAccount?.newMarketingLeader || '',
      'account.productMarketingPeople': fullAccount?.productMarketingPeople || '',
      'account.scoreSummary': fullAccount?.scoreSummary || '',
      'account.icpFit': fullAccount?.icpFit || '',
      'account.totalScore': String(fullAccount?.totalScore || 0),
      'account.scoreA': String(fullAccount?.scoreA || 0),
      'account.scoreB': String(fullAccount?.scoreB || 0),
      'account.scoreC': String(fullAccount?.scoreC || 0),
      'account.scoreD': String(fullAccount?.scoreD || 0),
      'account.scoreE': String(fullAccount?.scoreE || 0),
      'account.scoreF': String(fullAccount?.scoreF || 0),
      'account.aVerdict': fullAccount?.aVerdict || '',
      'account.bVerdict': fullAccount?.bVerdict || '',
      'account.cVerdict': fullAccount?.cVerdict || '',
      'account.dVerdict': fullAccount?.dVerdict || '',
      'account.eVerdict': fullAccount?.eVerdict || '',
      'account.fVerdict': fullAccount?.fVerdict || '',
      'account.homepageSections': fullAccount?.homepageSections || '',
      'account.homepageNav': fullAccount?.homepageNav || '',
      'account.productPages': fullAccount?.productPages || '',
      'account.newDirectionPage': fullAccount?.newDirectionPage || '',
      'account.scoreAJust': fullAccount?.scoreAJust || '',
      'account.scoreBJust': fullAccount?.scoreBJust || '',
      'account.scoreCJust': fullAccount?.scoreCJust || '',
      'account.scoreDJust': fullAccount?.scoreDJust || '',
      'account.scoreEJust': fullAccount?.scoreEJust || '',
      'account.scoreFJust': fullAccount?.scoreFJust || '',
      'account.scoreG': fullAccount?.scoreG || '',
      'account.scoreGJust': fullAccount?.scoreGJust || '',
      'account.gVerdict': fullAccount?.gVerdict || '',
    };
    return fields;
  };

  // Replace {{variable}} syntax in prompt
  const interpolatePrompt = (promptText, fields) => {
    return promptText.replace(/\{\{([^}]+)\}\}/g, (match, key) => {
      const trimmed = key.trim();
      return fields[trimmed] !== undefined ? fields[trimmed] : match;
    });
  };


  // === EMAIL TEMPLATES (use editable state) ===
  const getCredibility = (gapFactor) => ['B', 'C'].includes(gapFactor) ? emailTemplates.credibility_bc : emailTemplates.credibility_adg;

  const assembleEmail1 = (firstName, opening, gapFactor, buyerTitle) => {
    const cred = getCredibility(gapFactor);
    const cta = emailTemplates.cta.replace('{{buyerTitle}}', buyerTitle);
    return `${firstName},\n\n${opening}\n\n${cred}\n\n${cta}\n\n${emailTemplates.signoff}`;
  };

  const assembleEmail2 = (firstName, observation) => {
    return `${firstName},\n\n${observation}\n\n${emailTemplates.email2_closing}\n\n${emailTemplates.signoff}`;
  };

  // Generate messages for a single contact
  const generateForContact = async (contactId, { emailNum = null, e1Factor = '', e2Factor = '' } = {}) => {
    const cc = campaignContacts.find(c => c.contact_id === contactId);
    const ct = cc?.contacts;
    if (!ct) return null;

    const companyBasic = ct.companies;
    const fullAccount = companyBasic?.domain ? companies.find(c => c.domain === companyBasic.domain && c.status === 'complete') : null;
    if (!fullAccount) { addLog(`No scored account for ${ct.name}`); return null; }

    const firstName = (ct.name || '').split(' ')[0] || 'Hi';
    const companyName = fullAccount.companyName || '';
    
    // Compute top gaps from scores if not stored on the account
    let gap1Factor = e1Factor || fullAccount.gap1Factor || '';
    let gap2Factor = e2Factor || fullAccount.gap2Factor || '';
    if (!gap1Factor || !gap2Factor) {
      const scores = { A: fullAccount.scoreA || 0, B: fullAccount.scoreB || 0, C: fullAccount.scoreC || 0, D: fullAccount.scoreD || 0, E: fullAccount.scoreE || 0, F: fullAccount.scoreF || 0, G: fullAccount.scoreG || 0 };
      const sorted = Object.entries(scores).sort((a, b) => b[1] - a[1] || GAP_TIEBREAKER.indexOf(a[0]) - GAP_TIEBREAKER.indexOf(b[0]));
      if (!gap1Factor) gap1Factor = sorted[0]?.[0] || 'A';
      if (!gap2Factor) gap2Factor = sorted[1]?.[0] || 'B';
      addLog(`[${companyName}] Computed gaps from scores: ${gap1Factor} (+${scores[gap1Factor]}), ${gap2Factor} (+${scores[gap2Factor]})`);
    }

    // Extract verdict from factor JSON
    const getFactorVerdict = (factor) => {
      const justKey = `score${factor}Just`;
      const justStr = fullAccount[justKey] || '';
      try { const j = JSON.parse(justStr); return j.verdict || ''; } catch { return ''; }
    };
    const g1Verdict = getFactorVerdict(gap1Factor);
    const g2Verdict = getFactorVerdict(gap2Factor);

    const heroText = (fullAccount.homepageSections || '').substring(0, 500);
    const rawBuyer = fullAccount.targetDecisionMaker || fullAccount.targetCustomer || 'decision makers';
    // Strip research label prefixes like "**Primary Decision Maker Title:**" or "Primary Decision Maker:"
    const cleanedBuyer = rawBuyer
      .replace(/\*\*[^*]+\*\*:?\s*/g, '')  // remove **bold labels**
      .replace(/^(Primary |Target |)Decision Maker( Title)?:\s*/i, '')  // remove plain labels
      .trim();
    const targetBuyer = cleanedBuyer || 'decision makers';
    const buyerTitle = targetBuyer.split(',')[0].split('/')[0].split(' or ')[0].trim();

    // Training examples for observation quality
    const nearbyGroups = { D: ['D','G'], G: ['D','G'], E: ['D','E','G'], A: ['A','B','C'], B: ['A','B','C'], C: ['A','B','C'], F: ['E','F'] };
    const relevantFactors = new Set([gap1Factor, gap2Factor, ...(nearbyGroups[gap1Factor] || []), ...(nearbyGroups[gap2Factor] || [])].filter(Boolean));
    const matchedExamples = trainingExamples.filter(e => e.is_active !== false && relevantFactors.has(e.gap_factor)).slice(0, 6);

    let examplesSection = '';
    if (matchedExamples.length > 0) {
      examplesSection = matchedExamples.map(ex => {
          let s = `<example gap="${ex.gap_factor}">`;
          if (ex.company_name) s += `\n<company>${ex.company_name}</company>`;
          const openingText = ex.opening || ex.body || '';
          if (openingText) s += `\n<opening>${openingText}</opening>`;
          if (ex.context) s += `\n<context>${ex.context}</context>`;
          if (ex.avoid_notes) s += `\n<why_it_works>${ex.avoid_notes}</why_it_works>`;
          s += `\n</example>`;
          return s;
        }).join('\n\n');
    }

    // Extract full scoring JSON for a factor
    const getFactorData = (factor) => {
      const justKey = `score${factor}Just`;
      const justStr = fullAccount[justKey] || '';
      try { return JSON.parse(justStr); } catch { return {}; }
    };

    const buildObsPrompt = (gapFactor, emailNum) => {
      const gapPrompt = obsPrompts[gapFactor] || obsPrompts.A;
      const factorData = getFactorData(gapFactor);
      const verdict = factorData.verdict || '';

      // Factor-specific context assembly
      let contextBlock = '';
      if (gapFactor === 'A') {
        const diffs = (factorData.differentiators || []).map((d, i) => `${i+1}. ${d}`).join('\n');
        contextBlock = `<context>
Company: ${companyName}

Target buyer (use their role in the opening question, e.g. "Are [target buyers] lumping..."):
${targetBuyer}

Competitors (if confidence is high, name 1-2 in the email and state what they do wrong that this company does right. If confidence is low, describe the category without naming names):
${fullAccount.competitors || 'Not available'}

Differentiators (unique customer benefits that competitors cannot claim. Pick the SINGLE strongest one for the email):
${diffs || 'Not available'}

Verdict (summary of the gap between the unique value this company delivers to customers and how the homepage tells the story):
${verdict}

Homepage hero (first 300 chars):
${heroText.substring(0, 300)}
</context>`;
      } else {
        // Default context for other factors (B-G) — same as before
        contextBlock = `Company: ${companyName}
Contact: ${firstName} (${ct.title || 'executive'})
Verdict: ${verdict}
Homepage hero: ${heroText.substring(0, 300)}`;
      }

      // Email 1 vs Email 2 instructions
      if (emailNum === 1) {
        if (gapFactor === 'A') {
          // Factor A uses the full prompt with its own task/guidelines — just add context + examples + output format
          return `${gapPrompt}

${contextBlock}

<examples>
${examplesSection || 'No training examples available for this gap type.'}
</examples>

<output>
Return ONLY valid JSON:
{"opening": "The 3-4 sentences as one paragraph, exactly as they should appear in the email.", "subject": "2-5 word subject line that names the founder's pain or the buyer's blind spot"}
</output>`;
        } else {
          // Other factors use generic Email 1 instructions
          return `${gapPrompt}

${contextBlock}

Write an observation sentence and a consequence question for Email 1.

The observation is one sentence with two sides: what you see on the homepage vs the opportunity. Frame it POSITIVELY.

The consequence is one question about a real business friction. Frame it as easy to say yes to ("Do you think..." or "Do you feel like..."). It should name a downstream effect (deal velocity, buyer confusion, sales cycle), not restate the observation.

${OBS_SHARED_RULES}
${examplesSection}

Return ONLY JSON:
{"opening": "The observation and consequence as one flowing paragraph.", "subject": "2-5 word subject line"}`;
        }
      } else {
        // Email 2 — same for all factors
        return `${gapPrompt}

${contextBlock}

Write a short follow-up observation (one sentence) for Email 2. Start with "One other thing I noticed" or similar. One short sentence. Frame positively.

${OBS_SHARED_RULES}
${examplesSection}

Return ONLY JSON:
{"opening": "One short follow-up sentence.", "subject": "2-5 word subject line"}`;
      }
    };

    try {
      setGenProgress && setGenProgress('Generating email openings...');

      const parseResp = async (resp) => {
        if (!resp) return null;
        try {
          const data = await resp.json();
          if (data.error) { addLog(`API error: ${data.error}`); return null; }
          const text = (data.content || []).map(c => c.text || '').join('');
          if (!text) { addLog(`API returned empty text. Keys: ${Object.keys(data).join(',')}`); return null; }
          const cleaned = text.replace(/```json|```/g, '').trim();
          const parsed = JSON.parse(cleaned);
          if (parsed.opening) return parsed;
          if (parsed.observation) {
            return { opening: parsed.consequence ? `${parsed.observation} ${parsed.consequence}` : parsed.observation, subject: parsed.subject || '' };
          }
          addLog(`Unexpected response format: ${JSON.stringify(parsed).substring(0, 200)}`);
          return parsed;
        } catch (err) {
          addLog(`parseResp error: ${err.message}`);
          return null;
        }
      };

      // Generate one or both emails based on emailNum parameter
      const genEmail1 = !emailNum || emailNum === 1;
      const genEmail2 = !emailNum || emailNum === 2;

      addLog(`[${ct.name}] Generating: email1=${genEmail1} (factor=${gap1Factor}), email2=${genEmail2} (factor=${gap2Factor}), emailNum=${emailNum}`);

      // Capture inputs for display
      const factorData1 = genEmail1 ? getFactorData(gap1Factor) : null;
      const factorData2 = genEmail2 ? getFactorData(gap2Factor) : null;
      const inputSnapshot = {
        email1: genEmail1 ? {
          factor: gap1Factor,
          factorName: FACTOR_NAMES[gap1Factor] || gap1Factor,
          companyName,
          differentiators: factorData1?.differentiators || [],
          verdict: factorData1?.verdict || '',
          competitors: fullAccount.competitors || '',
          targetBuyer: fullAccount.targetDecisionMaker || fullAccount.targetCustomer || '',
          heroText: heroText.substring(0, 300),
          exampleCount: matchedExamples.filter(e => e.gap_factor === gap1Factor || (nearbyGroups[gap1Factor] || []).includes(e.gap_factor)).length,
        } : null,
        email2: genEmail2 ? {
          factor: gap2Factor,
          factorName: FACTOR_NAMES[gap2Factor] || gap2Factor,
          companyName,
          verdict: factorData2?.verdict || '',
          heroText: heroText.substring(0, 300),
        } : null,
      };
      setLastPromptInputs(inputSnapshot);

      const [resp1, resp2] = await Promise.all([
        genEmail1 && gap1Factor ? fetch('/api/claude', {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ prompt: buildObsPrompt(gap1Factor, 1), systemPrompt: 'You are a JSON-only API. Return only valid JSON.', model: 'claude-sonnet-4-20250514', maxTokens: 800 }),
        }) : Promise.resolve(null),
        genEmail2 && gap2Factor ? fetch('/api/claude', {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ prompt: buildObsPrompt(gap2Factor, 2), systemPrompt: 'You are a JSON-only API. Return only valid JSON.', model: 'claude-sonnet-4-20250514', maxTokens: 500 }),
        }) : Promise.resolve(null),
      ]);

      const obs1 = await parseResp(resp1);
      const obs2 = await parseResp(resp2);
      addLog(`[${ct.name}] Email 1: ${obs1 ? JSON.stringify(obs1).substring(0, 120) : 'null'}...`);
      addLog(`[${ct.name}] Email 2: ${obs2 ? JSON.stringify(obs2).substring(0, 120) : 'null'}...`);

      // Assemble emails with code
      const email1Body = obs1 ? assembleEmail1(firstName, obs1.opening, gap1Factor, buyerTitle) : null;
      const email2Body = obs2 ? assembleEmail2(firstName, obs2.opening) : null;

      const messages = [
        ...(email1Body ? [{ step_number: 1, channel: 'email', subject: obs1?.subject || '', body: email1Body }] : []),
        ...(email2Body ? [{ step_number: 2, channel: 'email', subject: obs2?.subject || '', body: email2Body }] : []),
      ];

      const gapSelection = {
        scores: { A: fullAccount.scoreA, B: fullAccount.scoreB, C: fullAccount.scoreC, D: fullAccount.scoreD, E: fullAccount.scoreE, F: fullAccount.scoreF, G: fullAccount.scoreG },
        email1_gap: gap1Factor, email1_gap_name: FACTOR_NAMES[gap1Factor], email1_score: fullAccount.gap1Score,
        email2_gap: gap2Factor, email2_gap_name: FACTOR_NAMES[gap2Factor], email2_score: fullAccount.gap2Score,
        reasoning: 'Top 2 gaps by score, tiebreaker G>D>E>B>A>C>F',
      };

      const draftMessages = [...messages];
      const reviewNotes = 'Credibility, CTA, and structure assembled by code. No review pass needed.';

      // Save to DB
      const saved = await upsertGeneratedMessages(activeCampaignId, contactId, messages);
      return { saved, draftMessages, reviewNotes, gapSelection };
    } catch (err) {
      addLog(`Generation error for ${ct.name}: ${err.message}`);
      return null;
    }
  };

  // Generate a single email (1 or 2) for the selected contact
  const handleGenerateSingleEmail = async (emailNum) => {
    if (!selectedCampaignContact) return;
    setGenerating(true);
    setGenProgress(`Generating Email ${emailNum}...`);
    const opts = {
      emailNum,
      e1Factor: email1FactorOverride || '',
      e2Factor: email2FactorOverride || '',
    };
    const result = await generateForContact(selectedCampaignContact, opts);
    if (result?.saved) {
      setContactGenMessages(result.saved);
      setContactDraftMessages(result.draftMessages || []);
      setContactReviewNotes(result.reviewNotes || '');
      setContactGapSelection(result.gapSelection || null);
      setGenContactsWithMessages(prev => new Set([...prev, selectedCampaignContact]));
    }
    setGenerating(false);
    setGenProgress('');
  };

  const handleTestOnMultiple = async (count) => {
    if (campaignContacts.length === 0) return;
    setGenerating(true);
    const contactIds = campaignContacts
      .map(cc => cc.contact_id)
      .slice(0, count === 'all' ? undefined : count);
    const total = contactIds.length;

    for (let i = 0; i < total; i++) {
      setGenProgress(`${i + 1}/${total}...`);
      // Bulk generate always uses the account's default gap1 and gap2 — no overrides
      const result = await generateForContact(contactIds[i], {});
      if (result?.saved) {
        setGenContactsWithMessages(prev => new Set([...prev, contactIds[i]]));
        if (contactIds[i] === selectedCampaignContact) {
          setContactGenMessages(result.saved);
          setContactDraftMessages(result.draftMessages || []);
          setContactReviewNotes(result.reviewNotes || '');
          setContactGapSelection(result.gapSelection || null);
        }
      }
      if (i < total - 1) await new Promise(r => setTimeout(r, 500));
    }
    setGenerating(false);
    setGenProgress('');
    addLog(`Generated messages for ${total} contacts`);
  };

  // Save an edited generated message
  const handleSaveGeneratedMessage = async (msg) => {
    try {
      const updated = await updateGeneratedMessage(msg.id, { subject: msg.subject, body: msg.body });
      setContactGenMessages(prev => prev.map(m => m.id === updated.id ? updated : m));
    } catch (err) { addLog(`Save generated message error: ${err.message}`); }
  };

  // Instantly: fetch campaigns list
  const fetchInstantlyCampaigns = async () => {
    try {
      const resp = await fetch('/api/instantly', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: 'list_campaigns' }),
      });
      const data = await resp.json();
      if (data.error) {
        setInstantlyResult({ type: 'error', message: data.error });
        return;
      }
      // Instantly v2 returns { data: [...] } or array directly
      const list = Array.isArray(data) ? data : (data.data || data.items || []);
      setInstantlyCampaigns(list);
    } catch (err) {
      setInstantlyResult({ type: 'error', message: err.message });
    }
  };

  // Instantly: push campaign contacts as leads
  const pushToInstantly = async (instantlyCampaignId) => {
    setInstantlyPicker(false);
    setInstantlyLoading(true);
    setInstantlyResult(null);

    try {
      // Get contacts with emails
      const contactsWithEmail = campaignContacts
        .map(cc => cc.contacts)
        .filter(ct => ct && ct.business_email);

      if (contactsWithEmail.length === 0) {
        setInstantlyResult({ type: 'error', message: 'No contacts with email addresses found in this campaign.' });
        setInstantlyLoading(false);
        return;
      }

      // Build email custom variables from campaign messages
      const emailVariables = {};
      const emailMessages = campaignMessages
        .filter(m => m.channel === 'email')
        .sort((a, b) => a.step_number - b.step_number);
      
      emailMessages.forEach((msg, idx) => {
        const num = idx + 1;
        emailVariables[`email_${num}_subject`] = msg.subject || '';
        emailVariables[`email_${num}_body`] = msg.body || '';
      });

      // Build leads array
      const leads = contactsWithEmail.map(ct => {
        // Split name into first/last
        const nameParts = (ct.name || '').trim().split(/\s+/);
        const firstName = nameParts[0] || '';
        const lastName = nameParts.slice(1).join(' ') || '';

        // Get company info
        const companyName = ct.companies?.name || '';

        return {
          email: ct.business_email,
          first_name: firstName,
          last_name: lastName,
          company_name: companyName,
          title: ct.title || '',
          linkedin_url: ct.linkedin || '',
          // Spread email templates as custom variables
          ...emailVariables,
        };
      });

      addLog(`[instantly] Pushing ${leads.length} leads with ${emailMessages.length} email templates...`);

      const resp = await fetch('/api/instantly', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          action: 'add_leads',
          campaign_id: instantlyCampaignId,
          leads,
        }),
      });

      const data = await resp.json();

      if (data.error) {
        setInstantlyResult({ type: 'error', message: data.error });
        addLog(`[instantly] Error: ${data.error}`);
      } else {
        const summary = `Pushed ${leads.length} leads with ${Object.keys(emailVariables).length / 2} email templates.`;
        setInstantlyResult({ type: 'success', message: summary });
        addLog(`[instantly] Success: ${summary}`);
      }
    } catch (err) {
      setInstantlyResult({ type: 'error', message: err.message });
      addLog(`[instantly] Error: ${err.message}`);
    }
    setInstantlyLoading(false);
  };

  const completedCount = companies.filter(c => c.status === 'complete').length;
  const errorCount = companies.filter(c => c.status === 'error').length;
  const strongCount = companies.filter(c => c.icpFit === 'Strong').length;
  const moderateCount = companies.filter(c => c.icpFit === 'Moderate').length;
  const fitColors = {
    'Strong': 'text-green-600 bg-green-50 border-green-300',
    'Moderate': 'text-amber-600 bg-amber-50 border-amber-300',
    'Weak': 'text-red-600 bg-red-50 border-red-300',
    'Disqualified': 'text-gray-500 bg-gray-500/10 border-gray-500/30',
  };

  const screenedCompanies = companies.filter(c => c.status === 'complete' || c.status === 'error');
  const databaseAccounts = screenedCompanies.filter(c => c.dbStatus === 'active');
  const stagedCompanies = screenedCompanies.filter(c => c.dbStatus === 'screened');
  const pendingCompanies = companies.filter(c => c.status === 'pending' || c.status === 'error' || c.status === 'processing');

  return (
    <div className="min-h-screen bg-gray-100">
      {/* Top navigation bar */}
      <div className="bg-white border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-6">
          {/* Row 1: Logo + Section tabs */}
          <div className="flex items-center py-3">
            <div className="flex items-center gap-3 mr-8">
              <div className="w-9 h-9 rounded-xl bg-sky-500 flex items-center justify-center overflow-hidden">
                <img src="/icon.png" alt="Strata" className="w-7 h-7 object-contain" />
              </div>
              <div>
                <div className="text-sm font-bold text-gray-900 leading-tight">Strata ICP Screener</div>
                <div className="text-[10px] text-gray-400 leading-tight">Narrative gap analysis for B2B SaaS</div>
              </div>
            </div>
            {(() => {
              const sections = [
                { key: 'discover', label: 'Discover', views: [
                  { key: 'discover_accounts', label: 'Accounts' },
                  { key: 'discover_contacts', label: 'Contacts' },
                  { key: 'discover_prompts', label: 'Research Prompt' },
                ]},
                { key: 'screening', label: 'Screening', views: [
                  { key: 'upload', label: 'Screen Queue', badge: pendingCompanies.length > 0 ? pendingCompanies.length : null },
                  { key: 'screened', label: 'Screened Accounts', badge: stagedCompanies.length > 0 ? stagedCompanies.length : null },
                  { key: 'email_gen', label: 'Email Generation' },
                  { key: 'prompt_settings', label: 'Prompt Settings' },
                ]},
                { key: 'database', label: 'Database', views: [
                  { key: 'accounts', label: 'Accounts' },
                  { key: 'contacts', label: 'Contacts' },
                  { key: 'campaigns', label: 'Campaigns' },
                  { key: 'email_prompts', label: 'Email Prompts' },
                ]},
              ];
              const currentSection = sections.find(s => s.views.some(v => v.key === activeView)) || sections[0];
              return (
                <nav className="flex items-center gap-1">
                  {sections.map(section => (
                    <button key={section.key} onClick={() => setActiveView(section.views[0].key)}
                      className={`px-4 py-2 text-sm font-semibold rounded-lg transition-all ${
                        currentSection.key === section.key
                          ? 'bg-violet-600 text-white shadow-sm'
                          : 'text-gray-500 hover:text-gray-700 hover:bg-gray-100'
                      }`}>
                      {section.label}
                    </button>
                  ))}
                </nav>
              );
            })()}
          </div>
          {/* Sub-tabs row */}
          {(() => {
            const sections = [
              { key: 'discover', views: [
                { key: 'discover_accounts', label: 'Accounts' },
                { key: 'discover_contacts', label: 'Contacts' },
                { key: 'discover_prompts', label: 'Research Prompt' },
              ]},
              { key: 'screening', views: [
                { key: 'upload', label: 'Screen Queue', badge: pendingCompanies.length > 0 ? pendingCompanies.length : null },
                { key: 'screened', label: 'Screened Accounts', badge: stagedCompanies.length > 0 ? stagedCompanies.length : null },
                { key: 'email_gen', label: 'Email Generation' },
                { key: 'prompt_settings', label: 'Prompt Settings' },
              ]},
              { key: 'database', views: [
                { key: 'accounts', label: 'Accounts' },
                { key: 'contacts', label: 'Contacts' },
                { key: 'campaigns', label: 'Campaigns' },
                { key: 'email_prompts', label: 'Email Prompts' },
              ]},
            ];
            const currentSection = sections.find(s => s.views.some(v => v.key === activeView)) || sections[0];
            return (
              <div className="max-w-7xl mx-auto px-6 py-2 border-t border-gray-100">
                <nav className="flex items-center gap-1">
                  {currentSection.views.map(tab => (
                    <button key={tab.key} onClick={() => setActiveView(tab.key)}
                      className={`px-3.5 py-1.5 text-[13px] font-medium rounded-md transition-all ${
                        activeView === tab.key
                          ? 'bg-gray-100 text-gray-900 border border-gray-200'
                          : 'text-gray-400 hover:text-gray-600'
                      }`}>
                      {tab.label}
                      {tab.badge && <span className="ml-1.5 px-1.5 py-0.5 bg-amber-100 text-amber-700 rounded-full text-[10px] font-bold">{tab.badge}</span>}
                    </button>
                  ))}
                </nav>
              </div>
            );
          })()}
        </div>
      </div>

      <div className="max-w-7xl mx-auto p-6">

        {error && <div className="mb-4 p-3 bg-red-50 border border-red-300 rounded text-red-600 text-sm">{error}</div>}


        {/* ======= ACCOUNTS VIEW (Split: list left, detail right) ======= */}
        {activeView === 'accounts' && (
          <div>
            {/* Search & Filter bar */}
            <div className="bg-white border border-gray-200 rounded-lg px-4 py-3 mb-3">
              <div className="flex items-center gap-3 flex-wrap">
                {/* Search */}
                <div className="relative flex-1 min-w-[180px] max-w-[320px]">
                  <input type="text" placeholder="Search accounts..."
                    value={accountSearch} onChange={e => setAccountSearch(e.target.value)}
                    className="w-full bg-gray-50 border border-gray-200 rounded-lg pl-8 pr-3 py-1.5 text-sm text-gray-700 placeholder-gray-400 focus:border-violet-300 focus:outline-none" />
                  <svg className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.3-4.3"/></svg>
                </div>
                {/* ICP Fit filter — multi-select pills */}
                <div className="flex items-center gap-1">
                  {['Strong', 'Moderate', 'Weak', 'Disqualified'].map(fit => (
                    <button key={fit} onClick={() => setAccountFitFilter(prev => prev.includes(fit) ? prev.filter(f => f !== fit) : [...prev, fit])}
                      className={`px-2 py-1 rounded-lg text-[11px] font-medium transition-colors ${
                        accountFitFilter.includes(fit)
                          ? fit === 'Strong' ? 'bg-green-100 text-green-700 border border-green-300'
                            : fit === 'Moderate' ? 'bg-amber-100 text-amber-700 border border-amber-300'
                            : fit === 'Weak' ? 'bg-red-100 text-red-600 border border-red-300'
                            : 'bg-gray-200 text-gray-600 border border-gray-300'
                          : 'bg-white text-gray-400 border border-gray-200 hover:border-gray-300'
                      }`}>
                      {fit}
                    </button>
                  ))}
                </div>
                {/* Status filter — multi-select pills */}
                <div className="flex items-center gap-1">
                  {ACCOUNT_STATUSES.map(status => (
                    <button key={status} onClick={() => setAccountStatusFilter(prev => prev.includes(status) ? prev.filter(s => s !== status) : [...prev, status])}
                      className={`px-2 py-1 rounded-lg text-[11px] font-medium transition-colors ${
                        accountStatusFilter.includes(status)
                          ? status === 'Cold' ? 'bg-gray-200 text-gray-600 border border-gray-300'
                            : status === 'Engaged' ? 'bg-blue-100 text-blue-600 border border-blue-300'
                            : status === 'Opportunity' ? 'bg-amber-100 text-amber-600 border border-amber-300'
                            : 'bg-green-100 text-green-600 border border-green-300'
                          : 'bg-white text-gray-400 border border-gray-200 hover:border-gray-300'
                      }`}>
                      {status}
                    </button>
                  ))}
                </div>
                {/* Sort */}
                <select value={accountSort} onChange={e => setAccountSort(e.target.value)}
                  className="bg-gray-50 border border-gray-200 rounded-lg px-3 py-1.5 text-xs text-gray-600 focus:border-violet-300 focus:outline-none">
                  <option value="score">Sort: Score ↓</option>
                  <option value="name">Sort: Name A-Z</option>
                  <option value="contacts">Sort: Contacts ↓</option>
                  <option value="status">Sort: Status</option>
                  <option value="newest">Sort: Newest</option>
                  <option value="screened">Sort: Last Screened</option>
                </select>
                {/* Date filters */}
                <div className="flex items-center gap-1.5">
                  <label className="text-[10px] text-gray-400">Added after</label>
                  <input type="date" value={accountAddedAfter} onChange={e => setAccountAddedAfter(e.target.value)}
                    className="bg-gray-50 border border-gray-200 rounded-lg px-2 py-1 text-xs text-gray-600 focus:border-violet-300 focus:outline-none" />
                </div>
                <div className="flex items-center gap-1.5">
                  <label className="text-[10px] text-gray-400">Screened after</label>
                  <input type="date" value={accountScreenedAfter} onChange={e => setAccountScreenedAfter(e.target.value)}
                    className="bg-gray-50 border border-gray-200 rounded-lg px-2 py-1 text-xs text-gray-600 focus:border-violet-300 focus:outline-none" />
                </div>
                {(accountAddedAfter || accountScreenedAfter) && (
                  <button onClick={() => { setAccountAddedAfter(''); setAccountScreenedAfter(''); }} className="text-[10px] text-gray-400 hover:text-gray-600">Clear dates</button>
                )}
                {/* Hide with contacts toggle */}
                <label className="flex items-center gap-1.5 text-xs text-gray-500 cursor-pointer select-none ml-auto">
                  <input type="checkbox" checked={hideAccountsWithContacts} onChange={e => setHideAccountsWithContacts(e.target.checked)} className="accent-violet-500" />
                  Hide with contacts
                </label>
              </div>
              {/* Bulk actions bar — appears when accounts are selected */}
              {checkedAccounts.size > 0 && (
                <div className="flex items-center gap-2 mt-3 pt-3 border-t border-gray-100">
                  <span className="text-xs text-violet-600 font-medium">{checkedAccounts.size} selected</span>
                  <button onClick={batchFindContacts} className="px-3 py-1.5 bg-violet-600 hover:bg-violet-500 text-white rounded-lg text-xs font-medium">Find Contacts</button>
                  {/* Bulk status change */}
                  <div className="relative">
                    <button onClick={() => setBulkStatusPicker(!bulkStatusPicker)} className="px-3 py-1.5 bg-white hover:bg-gray-50 text-gray-600 border border-gray-200 rounded-lg text-xs font-medium">Set Status ▾</button>
                    {bulkStatusPicker && (
                      <div className="absolute left-0 top-9 z-50 bg-white border border-gray-200 rounded-lg shadow-xl py-1 min-w-[140px]">
                        {ACCOUNT_STATUSES.map(s => (
                          <button key={s} onClick={() => bulkUpdateAccountStatus(s)}
                            className="block w-full text-left px-4 py-2 text-[13px] text-gray-600 hover:bg-violet-50 hover:text-violet-600">{s}</button>
                        ))}
                      </div>
                    )}
                  </div>
                  <button onClick={exportAccountsCSV} className="px-3 py-1.5 bg-white hover:bg-gray-50 text-gray-600 border border-gray-200 rounded-lg text-xs font-medium">Export CSV</button>
                  <button onClick={handleBulkDeleteAccounts} className="px-3 py-1.5 text-red-500 hover:text-red-700 border border-red-200 hover:bg-red-50 rounded-lg text-xs font-medium">Delete</button>
                  <button onClick={() => { setCheckedAccounts(new Set()); setBulkStatusPicker(false); }} className="text-xs text-gray-400 hover:text-gray-600 ml-1">Clear</button>
                </div>
              )}
            </div>
            {/* Stats row */}
            <div className="mb-3 flex items-center gap-4 text-xs text-gray-500">
              <span>{databaseAccounts.length} accounts</span>
              {strongCount > 0 && <span className="text-green-600">{strongCount} strong</span>}
              {moderateCount > 0 && <span className="text-amber-600">{moderateCount} moderate</span>}
              {databaseAccounts.filter(c => (c.accountStatus || 'Cold') !== 'Cold').length > 0 && (
                <>
                  {ACCOUNT_STATUSES.filter(s => s !== 'Cold').map(s => {
                    const cnt = databaseAccounts.filter(c => c.accountStatus === s).length;
                    return cnt > 0 ? <span key={s}>{cnt} {s.toLowerCase()}</span> : null;
                  })}
                </>
              )}
              {!checkedAccounts.size && completedCount > 0 && (
                <button onClick={exportAccountsCSV} className="ml-auto px-3 py-1 bg-white hover:bg-gray-50 text-gray-500 border border-gray-200 rounded-lg text-xs">Export All</button>
              )}
            </div>
          <div className="flex gap-4" style={{ minHeight: '70vh' }}>
            {/* Left: Account list */}
            <div className={`${selectedCompany !== null ? 'w-1/4 min-w-[220px]' : 'w-full'} flex flex-col`}>
              {(() => {
                // Apply filters
                let filtered = databaseAccounts;
                if (accountSearch) {
                  const q = accountSearch.toLowerCase();
                  filtered = filtered.filter(c => (c.companyName || '').toLowerCase().includes(q) || (c.domain || '').toLowerCase().includes(q));
                }
                if (accountFitFilter.length > 0) filtered = filtered.filter(c => accountFitFilter.includes(c.icpFit));
                if (accountStatusFilter.length > 0) filtered = filtered.filter(c => accountStatusFilter.includes(c.accountStatus || 'Cold'));
                if (hideAccountsWithContacts) filtered = filtered.filter(c => allContacts.filter(ct => ct.company_domain === c.domain).length === 0);
                if (accountAddedAfter) filtered = filtered.filter(c => c.addedAt && new Date(c.addedAt) >= new Date(accountAddedAfter));
                if (accountScreenedAfter) filtered = filtered.filter(c => c.lastScreenedAt && new Date(c.lastScreenedAt) >= new Date(accountScreenedAfter));
                
                // Apply sort
                const getContactCount = (c) => allContacts.filter(ct => ct.company_domain === c.domain).length;
                const statusOrder = { 'Client': 0, 'Opportunity': 1, 'Engaged': 2, 'Cold': 3 };
                filtered = [...filtered].sort((a, b) => {
                  if (accountSort === 'score') return (b.totalScore || 0) - (a.totalScore || 0);
                  if (accountSort === 'name') return (a.companyName || '').localeCompare(b.companyName || '');
                  if (accountSort === 'contacts') return getContactCount(b) - getContactCount(a);
                  if (accountSort === 'status') return (statusOrder[a.accountStatus || 'Cold'] || 3) - (statusOrder[b.accountStatus || 'Cold'] || 3);
                  if (accountSort === 'newest') return new Date(b.addedAt || 0) - new Date(a.addedAt || 0);
                  if (accountSort === 'screened') return new Date(b.lastScreenedAt || 0) - new Date(a.lastScreenedAt || 0);
                  return 0;
                });

                const statusColors = { 'Cold': 'bg-gray-100 text-gray-500', 'Engaged': 'bg-blue-50 text-blue-600', 'Opportunity': 'bg-amber-50 text-amber-600', 'Client': 'bg-green-50 text-green-600' };
                
                return (
                  <>
                    {filtered.length > 0 && (
                      <div className="flex items-center gap-2 px-4 py-1.5 mb-1 text-xs text-gray-500">
                        <input type="checkbox"
                          checked={filtered.length > 0 && filtered.every(c => checkedAccounts.has(c.domain))}
                          onChange={(e) => {
                            if (e.target.checked) setCheckedAccounts(new Set(filtered.map(c => c.domain)));
                            else setCheckedAccounts(new Set());
                          }}
                          className="accent-violet-500 cursor-pointer" />
                        <span>Select all ({filtered.length})</span>
                      </div>
                    )}
                    <div className="flex-1 overflow-auto space-y-1.5">
                      {filtered.length > 0 ? filtered.map((c) => {
                        const origIdx = companies.indexOf(c);
                        const isSelected = selectedCompany === origIdx;
                        const isChecked = checkedAccounts.has(c.domain);
                        const contactCount = getContactCount(c);
                        const acctStatus = c.accountStatus || 'Cold';
                        return (
                          <div key={c.domain || origIdx}
                            className={`px-4 py-2.5 rounded-lg cursor-pointer transition-colors border ${
                              isChecked ? 'bg-violet-50 border-violet-300' : isSelected ? 'bg-violet-50 border-violet-300' : 'bg-white border-gray-200 hover:border-gray-300'
                            }`}>
                            <div className="flex items-center gap-3">
                              <input type="checkbox" checked={isChecked}
                                onChange={(e) => {
                                  e.stopPropagation();
                                  setCheckedAccounts(prev => {
                                    const next = new Set(prev);
                                    if (next.has(c.domain)) next.delete(c.domain); else next.add(c.domain);
                                    return next;
                                  });
                                }}
                                className="accent-violet-500 cursor-pointer flex-shrink-0" />
                              <div className="flex-1 min-w-0" onClick={() => setSelectedCompany(isSelected ? null : origIdx)}>
                                <div className="flex items-center gap-2">
                                  <span className={`inline-block w-7 h-7 rounded-full leading-7 text-center font-bold text-xs flex-shrink-0 ${c.totalScore >= 16 ? 'bg-green-100 text-green-600' : c.totalScore >= 11 ? 'bg-amber-100 text-amber-600' : 'bg-red-100 text-red-600'}`}>{c.totalScore}</span>
                                  <span className="font-medium text-gray-900 text-sm truncate">{c.companyName}</span>
                                  {c.icpFit && <span className={`px-1.5 py-0.5 rounded text-[10px] border ${fitColors[c.icpFit] || ''}`}>{c.icpFit}</span>}
                                  <span className={`px-1.5 py-0.5 rounded text-[10px] ${statusColors[acctStatus] || statusColors['Cold']}`}>{acctStatus}</span>
                                </div>
                                <div className="text-[10px] text-gray-500 mt-0.5 ml-9">{c.domain}{contactCount > 0 ? ` · ${contactCount} contacts` : ''}</div>
                              </div>
                              {selectedCompany === null && <div className="text-[10px] text-gray-400 flex-shrink-0">A:{c.scoreA} B:{c.scoreB} C:{c.scoreC} D:{c.scoreD} E:{c.scoreE} F:{c.scoreF}</div>}
                            </div>
                          </div>
                        );
                      }) : (
                        <div className="text-center py-20">
                          <div className="text-gray-500 text-lg mb-2">{accountSearch || accountFitFilter.length > 0 || accountStatusFilter.length > 0 ? 'No matching accounts' : hideAccountsWithContacts ? 'All accounts have contacts' : 'No accounts yet'}</div>
                          <div className="text-gray-400 text-sm">{accountSearch || accountFitFilter.length > 0 || accountStatusFilter.length > 0 ? 'Try adjusting your filters.' : hideAccountsWithContacts ? 'Uncheck "Hide with contacts" to see all.' : 'Switch to Screen or Discover to add companies.'}</div>
                        </div>
                      )}
                    </div>
                  </>
                );
              })()}
              </div>
            
            {/* Right: Account detail */}
            {selectedCompany !== null && (() => {
              const c = companies[selectedCompany];
              if (!c || c.status !== 'complete') return null;
              const origIdx = selectedCompany;
              const accountContacts = allContacts.filter(ct => ct.company_domain === c.domain);
              return (
                <div className="flex-1 bg-white border border-gray-200 rounded-lg overflow-auto" style={{ maxHeight: '85vh' }}>
                  {/* Header */}
                  <div className="px-5 py-4 border-b border-gray-100 sticky top-0 bg-white/95 backdrop-blur z-10">
                    <div className="flex items-center justify-between">
                      <div>
                        <h2 className="text-lg font-bold text-gray-900">{c.companyName}</h2>
                        <div className="text-xs text-gray-500 mt-0.5">
                          <a href={c.website} target="_blank" rel="noopener noreferrer" className="text-violet-600 hover:text-violet-700">{c.domain}</a>
                          <span className="mx-2">·</span>
                          <span className={`font-medium ${c.totalScore >= 16 ? 'text-green-600' : c.totalScore >= 11 ? 'text-amber-600' : 'text-red-600'}`}>{c.totalScore}/21</span>
                          {c.icpFit && <span className={`ml-2 px-1.5 py-0.5 rounded text-[10px] border ${fitColors[c.icpFit] || ''}`}>{c.icpFit}</span>}
                        </div>
                      </div>
                      <div className="flex items-center gap-2">
                        <select value={c.accountStatus || 'Cold'} onChange={e => updateAccountStatus(origIdx, e.target.value)}
                          className="bg-gray-50 border border-gray-200 rounded-lg px-2.5 py-1.5 text-xs text-gray-600 focus:border-violet-300 focus:outline-none">
                          {ACCOUNT_STATUSES.map(s => <option key={s} value={s}>{s}</option>)}
                        </select>
                        <button onClick={() => openContactsModal({ domain: c.domain || normalizeDomain(c.website), name: c.companyName })}
                          className="px-3 py-1.5 bg-violet-600 hover:bg-violet-500 text-white rounded-lg text-xs font-medium">Find Contacts</button>
                        <button onClick={() => {
                          const idx = origIdx;
                          setCompanies(prev => {
                            const u = [...prev];
                            u[idx] = { ...u[idx], status: 'pending', step: '', scoringResult: '', researchResult: '',
                              scoreA: 0, scoreB: 0, scoreC: 0, scoreD: 0, scoreE: 0, scoreF: 0, scoreG: 0,
                              scoreAJust: '', scoreBJust: '', scoreCJust: '', scoreDJust: '', scoreEJust: '', scoreFJust: '', scoreGJust: '',
                              totalScore: 0, icpFit: '', scoreSummary: '',
                              gap1Factor: '', gap1Name: '', gap1Score: 0, gap1Opportunity: '',
                              gap2Factor: '', gap2Name: '', gap2Score: 0, gap2Opportunity: '' };
                            return u;
                          });
                          // Reset DB run so a new one is created
                          setCompanies(prev => {
                            const u = [...prev];
                            u[idx] = { ...u[idx], dbRunId: null };
                            return u;
                          });
                          processCompany(idx);
                        }}
                          className="px-3 py-1.5 bg-amber-50 hover:bg-amber-100 text-amber-700 rounded-lg text-xs font-medium border border-amber-200">Re-screen</button>
                        <button onClick={() => setSelectedCompany(null)} className="text-gray-500 hover:text-gray-900 text-lg">×</button>
                      </div>
                    </div>
                  </div>
                  
                  {/* Screening Warnings */}
                  {c.screeningWarnings && c.screeningWarnings.length > 0 && (
                    <div className="px-5 py-3 border-b border-amber-100 bg-amber-50/30">
                      <details>
                        <summary className="flex items-center gap-2 cursor-pointer select-none">
                          <span className="text-[10px] text-amber-600 uppercase tracking-wide font-semibold">⚠ Screening Warnings ({c.screeningWarnings.length})</span>
                        </summary>
                        <div className="mt-2 space-y-1">
                          {c.screeningWarnings.map((w, i) => (
                            <div key={i} className="text-[11px] text-amber-700 flex items-start gap-1.5">
                              <span className="text-amber-400 mt-0.5">•</span>
                              <span>{w}</span>
                            </div>
                          ))}
                        </div>
                      </details>
                    </div>
                  )}

                  {/* Top Gaps — Opportunity framing */}
                  {(c.gap1Opportunity || c.gap2Opportunity) && (
                    <div className="px-5 py-4 border-b border-gray-100 space-y-3">
                      {c.gap1Opportunity && (
                        <div className="p-3 bg-violet-50/50 rounded-lg border border-violet-200/50">
                          <div className="flex items-center gap-2 mb-1.5">
                            <span className="text-[10px] bg-violet-600 text-white px-1.5 py-0.5 rounded font-bold">①</span>
                            <span className="text-xs text-violet-700 font-semibold">{c.gap1Factor}. {c.gap1Name}</span>
                            <span className="text-[10px] text-violet-500">+{c.gap1Score}</span>
                          </div>
                          <div className="text-xs text-gray-600 leading-relaxed">{c.gap1Opportunity}</div>
                        </div>
                      )}
                      {c.gap2Opportunity && (
                        <div className="p-3 bg-violet-50/30 rounded-lg border border-violet-200/30">
                          <div className="flex items-center gap-2 mb-1.5">
                            <span className="text-[10px] bg-violet-400 text-white px-1.5 py-0.5 rounded font-bold">②</span>
                            <span className="text-xs text-violet-600 font-semibold">{c.gap2Factor}. {c.gap2Name}</span>
                            <span className="text-[10px] text-violet-400">+{c.gap2Score}</span>
                          </div>
                          <div className="text-xs text-gray-600 leading-relaxed">{c.gap2Opportunity}</div>
                        </div>
                      )}
                    </div>
                  )}
                  
                  {/* Scoring breakdown — gap #1 and #2 first, then rest */}
                  {c.scoringResult && (
                    <div className="px-5 py-4 border-b border-gray-100">
                      <div className="flex items-center justify-between mb-3">
                        <div className="text-[9px] text-gray-500 uppercase tracking-wider font-semibold">Scoring Breakdown</div>
                        <span className={`font-bold text-sm ${c.totalScore >= 16 ? 'text-green-600' : c.totalScore >= 11 ? 'text-amber-600' : 'text-red-600'}`}>{c.totalScore}/21</span>
                      </div>
                      <div className="space-y-4">
                        {(() => {
                          const allFactors = [
                            { key: 'A', label: 'A. Differentiation', score: c.scoreA, just: c.scoreAJust, color: 'text-purple-400', borderColor: 'border-purple-500/20' },
                            { key: 'B', label: 'B. Outcomes', score: c.scoreB, just: c.scoreBJust, color: 'text-rose-600', borderColor: 'border-rose-500/20' },
                            { key: 'C', label: 'C. Customer-centric', score: c.scoreC, just: c.scoreCJust, color: 'text-orange-400', borderColor: 'border-orange-500/20' },
                            { key: 'D', label: 'D. Product change', score: c.scoreD, just: c.scoreDJust, color: 'text-cyan-400', borderColor: 'border-cyan-500/20' },
                            { key: 'E', label: 'E. Audience change', score: c.scoreE, just: c.scoreEJust, color: 'text-sky-600', borderColor: 'border-sky-500/20' },
                            { key: 'F', label: 'F. Multi-product', score: c.scoreF, just: c.scoreFJust, color: 'text-violet-600', borderColor: 'border-violet-500/20' },
                            { key: 'G', label: 'G. Vision Gap', score: c.scoreG, just: c.scoreGJust, color: 'text-pink-600', borderColor: 'border-pink-500/20' },
                          ];
                          const g1 = c.gap1Factor ? allFactors.find(f => f.key === c.gap1Factor) : null;
                          const g2 = c.gap2Factor ? allFactors.find(f => f.key === c.gap2Factor) : null;
                          const topKeys = new Set([c.gap1Factor, c.gap2Factor].filter(Boolean));
                          const rest = allFactors.filter(f => !topKeys.has(f.key));
                          const ordered = [...(g1 ? [g1] : []), ...(g2 ? [g2] : []), ...rest];
                          return ordered.map(({ key, label, score, just, color, borderColor }) => {
                            const isTop = topKeys.has(key);
                            const gapNum = key === c.gap1Factor ? '①' : key === c.gap2Factor ? '②' : null;
                            return (
                              <div key={key}>
                                <div className="flex items-center gap-2 mb-1.5">
                                  {gapNum && <span className="text-[10px] bg-violet-600 text-white px-1 py-0.5 rounded font-bold leading-none">{gapNum}</span>}
                                  <span className={`${color} font-semibold text-xs`}>{label}</span>
                                  <span className="flex gap-0.5">
                                    {[1,2,3].map(n => (
                                      <span key={n} className={`inline-block w-3.5 h-1.5 rounded-full ${n <= score ? (score === 3 ? 'bg-green-400' : score === 2 ? 'bg-amber-400' : 'bg-red-400') : 'bg-gray-200'}`} />
                                    ))}
                                  </span>
                                  <span className="text-gray-500 text-[10px]">+{score}</span>
                                </div>
                                <div className={`p-2.5 bg-gray-50/60 rounded-md border ${isTop ? 'border-violet-300/40' : borderColor}`}>
                                  <FactorPanel factorKey={key} data={just} />
                                </div>
                              </div>
                            );
                          });
                        })()}
                      </div>
                    </div>
                  )}
                  
                  {/* Contacts section */}
                  <div className="px-5 py-4 border-b border-gray-100">
                    <div className="flex items-center justify-between mb-3">
                      <div className="text-xs text-gray-500 uppercase tracking-wide font-medium">Contacts ({accountContacts.length})</div>
                      {accountContacts.filter(ct => !ct.business_email && ct.linkedin).length > 0 && (
                        <button onClick={() => enrichContactsBulk(accountContacts)}
                          className="px-2.5 py-1 bg-violet-50 hover:bg-violet-100 text-violet-600 rounded-lg text-[10px] font-medium border border-violet-200">
                          ✉ Enrich Emails ({accountContacts.filter(ct => !ct.business_email && ct.linkedin).length})
                        </button>
                      )}
                    </div>
                    {accountContacts.length > 0 ? (
                      <div className="space-y-1.5">
                        {accountContacts.slice(0, 20).map(ct => (
                          <div key={ct.id} className="flex items-center gap-3 px-3 py-2 bg-gray-100 rounded hover:bg-gray-100 cursor-pointer"
                            onClick={() => { setSelectedContactId(ct.id); setActiveView('contacts'); }}>
                            <div className="flex-1 min-w-0">
                              <div className="text-sm text-gray-900 font-medium truncate">{ct.name}</div>
                              <div className="text-xs text-gray-500 truncate">{ct.title}</div>
                            </div>
                            <div className="text-xs text-gray-400">{ct.seniority}</div>
                            {ct.business_email && <span className="text-[10px] text-emerald-600 truncate max-w-[140px]" title={ct.business_email}>✉ {ct.business_email}</span>}
                            {!ct.business_email && ct.email_verified && <span className="text-[10px] text-emerald-600/60">✓ verified</span>}
                            {ct.linkedin && <a href={ct.linkedin} target="_blank" rel="noopener noreferrer" onClick={e => e.stopPropagation()} className="text-violet-600 hover:text-violet-700 text-xs">LI</a>}
                          </div>
                        ))}
                      </div>
                    ) : (
                      <div className="text-xs text-gray-400">No contacts yet. Use "Find Contacts" to search via Crustdata.</div>
                    )}
                  </div>
                  
                  {/* Research report */}
                  <div className="px-5 py-3 border-b border-gray-100">
                    <ResearchReport company={c} />
                  </div>
                </div>
              );
            })()}
          </div>
          </div>
        )}

        {/* ======= CONTACTS VIEW (Split: list left, detail right) ======= */}
        {activeView === 'contacts' && (
          <div>
            {/* Toolbar — search, filters, actions */}
            <div className="mb-3 space-y-2">
              <div className="flex items-center gap-3 text-sm flex-wrap">
                {/* Search */}
                <div className="relative flex-1 min-w-[200px] max-w-[320px]">
                  <input type="text" placeholder="Search name, title, company..."
                    value={contactSearch} onChange={e => setContactSearch(e.target.value)}
                    className="w-full bg-gray-50 border border-gray-200 rounded-lg pl-8 pr-3 py-1.5 text-sm text-gray-700 placeholder-gray-400 focus:border-violet-300 focus:outline-none" />
                  <svg className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.3-4.3"/></svg>
                </div>
                <span className="text-gray-500 text-xs">{allContacts.length} contacts</span>
                {checkedContacts.size > 0 && (
                  <span className="text-violet-600 text-xs">{checkedContacts.size} selected</span>
                )}
                <div className="ml-auto flex items-center gap-2">
                  {checkedContacts.size > 0 && (
                    <>
                      <div className="relative">
                        <button onClick={() => setBulkCampaignPicker(!bulkCampaignPicker)} className="px-3 py-1.5 bg-violet-600 hover:bg-violet-500 text-white rounded-lg text-xs font-medium">+ Campaign</button>
                        {bulkCampaignPicker && (
                          <div className="absolute right-0 top-8 z-50 bg-white border border-gray-200 rounded-lg shadow-xl py-1 min-w-[200px]">
                            {campaigns.length > 0 ? campaigns.map(c => (
                              <button key={c.id} onClick={() => handleBulkAddToCampaign(c.id)}
                                className="block w-full text-left px-4 py-2 text-sm text-gray-600 hover:bg-violet-50 hover:text-violet-600">
                                {c.name}
                              </button>
                            )) : (
                              <div className="px-4 py-2 text-xs text-gray-500">No campaigns yet.</div>
                            )}
                          </div>
                        )}
                      </div>
                      <button onClick={handleBulkDeleteContacts} className="px-3 py-1.5 text-red-500 hover:text-red-700 border border-red-200 hover:bg-red-50 rounded-lg text-xs font-medium">Delete</button>
                      <button onClick={() => setCheckedContacts(new Set())} className="text-xs text-gray-400 hover:text-gray-600">Clear</button>
                    </>
                  )}
                  {databaseAccounts.length > 0 && (
                    <div className="relative">
                      <button onClick={() => setFindContactsPicker(findContactsPicker === 'contacts' ? null : 'contacts')} className="px-3 py-1.5 bg-violet-600 hover:bg-violet-500 text-white rounded-lg text-xs font-medium">Find More Contacts ▾</button>
                      {findContactsPicker === 'contacts' && (
                        <div className="absolute right-0 top-8 z-50 bg-white border border-gray-200 rounded-lg shadow-xl py-1 min-w-[220px] max-h-[300px] overflow-auto">
                          <div className="px-3 py-1.5 text-[10px] text-gray-400 uppercase tracking-wide">Select account</div>
                          {databaseAccounts.map(c => (
                            <button key={c.domain} onClick={() => { setFindContactsPicker(null); openContactsModal({ domain: c.domain, name: c.companyName }); }}
                              className="block w-full text-left px-4 py-2 text-sm text-gray-600 hover:bg-violet-50 hover:text-violet-600 truncate">
                              {c.companyName} <span className="text-gray-400 text-xs">({c.domain})</span>
                            </button>
                          ))}
                        </div>
                      )}
                    </div>
                  )}
                </div>
              </div>
              {/* Filter row */}
              <div className="flex items-center gap-2 flex-wrap text-xs">
                {/* Contact Status */}
                <div className="flex items-center gap-1">
                  {CONTACT_STATUSES.map(s => (
                    <button key={s} onClick={() => setContactStatusFilter(prev => prev.includes(s) ? prev.filter(x => x !== s) : [...prev, s])}
                      className={`px-2 py-0.5 rounded text-[11px] font-medium transition-colors ${
                        contactStatusFilter.includes(s)
                          ? s === 'New' ? 'bg-gray-200 text-gray-700 border border-gray-300'
                            : s === 'Engaged' ? 'bg-blue-100 text-blue-600 border border-blue-300'
                            : s === 'Opportunity' ? 'bg-amber-100 text-amber-600 border border-amber-300'
                            : 'bg-green-100 text-green-600 border border-green-300'
                          : 'bg-white text-gray-400 border border-gray-200 hover:border-gray-300'
                      }`}>{s}</button>
                  ))}
                </div>
                <span className="text-gray-300">|</span>
                {/* Email status */}
                <div className="flex items-center gap-1">
                  {[{ key: 'has_email', label: 'Has Email' }, { key: 'no_email', label: 'No Email' }, { key: 'enriched', label: 'Enriched' }].map(({ key, label }) => (
                    <button key={key} onClick={() => setContactEmailFilter(prev => prev.includes(key) ? prev.filter(x => x !== key) : [...prev, key])}
                      className={`px-2 py-0.5 rounded text-[11px] font-medium transition-colors ${
                        contactEmailFilter.includes(key) ? 'bg-violet-100 text-violet-600 border border-violet-300' : 'bg-white text-gray-400 border border-gray-200 hover:border-gray-300'
                      }`}>{label}</button>
                  ))}
                </div>
                <span className="text-gray-300">|</span>
                {/* Seniority */}
                {(() => {
                  const seniorities = [...new Set(allContacts.map(c => c.seniority).filter(Boolean))].sort();
                  return seniorities.length > 0 && (
                    <div className="flex items-center gap-1">
                      <span className="text-gray-400 text-[10px]">Seniority:</span>
                      {seniorities.map(s => (
                        <button key={s} onClick={() => setContactSeniorityFilter(prev => prev.includes(s) ? prev.filter(x => x !== s) : [...prev, s])}
                          className={`px-1.5 py-0.5 rounded text-[10px] font-medium transition-colors ${
                            contactSeniorityFilter.includes(s) ? 'bg-violet-100 text-violet-600 border border-violet-300' : 'bg-white text-gray-400 border border-gray-200 hover:border-gray-300'
                          }`}>{s}</button>
                      ))}
                    </div>
                  );
                })()}
                {/* Function */}
                {(() => {
                  const functions = [...new Set(allContacts.map(c => c.function_category).filter(Boolean))].sort();
                  return functions.length > 0 && (
                    <div className="flex items-center gap-1">
                      <span className="text-gray-400 text-[10px]">Function:</span>
                      {functions.slice(0, 6).map(f => (
                        <button key={f} onClick={() => setContactFunctionFilter(prev => prev.includes(f) ? prev.filter(x => x !== f) : [...prev, f])}
                          className={`px-1.5 py-0.5 rounded text-[10px] font-medium transition-colors ${
                            contactFunctionFilter.includes(f) ? 'bg-violet-100 text-violet-600 border border-violet-300' : 'bg-white text-gray-400 border border-gray-200 hover:border-gray-300'
                          }`}>{f}</button>
                      ))}
                    </div>
                  );
                })()}
                {/* Campaign filter */}
                {campaigns.length > 0 && (
                  <div className="flex items-center gap-1">
                    <span className="text-gray-400 text-[10px]">Campaign:</span>
                    {campaigns.map(camp => (
                      <button key={camp.id} onClick={() => setContactCampaignFilter(prev => prev.includes(camp.id) ? prev.filter(x => x !== camp.id) : [...prev, camp.id])}
                        className={`px-1.5 py-0.5 rounded text-[10px] font-medium transition-colors truncate max-w-[100px] ${
                          contactCampaignFilter.includes(camp.id) ? 'bg-violet-100 text-violet-600 border border-violet-300' : 'bg-white text-gray-400 border border-gray-200 hover:border-gray-300'
                        }`}>{camp.name}</button>
                    ))}
                  </div>
                )}
                {/* Sort */}
                <select value={contactSort} onChange={e => setContactSort(e.target.value)}
                  className="ml-auto bg-gray-50 border border-gray-200 rounded-lg px-2 py-1 text-xs text-gray-600 focus:border-violet-300 focus:outline-none">
                  <option value="newest">Sort: Newest</option>
                  <option value="name">Sort: Name A-Z</option>
                  <option value="company">Sort: Company</option>
                  <option value="status">Sort: Status</option>
                  <option value="last_enriched">Sort: Last Enriched</option>
                  <option value="last_campaign">Sort: Last Campaign Add</option>
                </select>
                {(contactSearch || contactStatusFilter.length > 0 || contactEmailFilter.length > 0 || contactSeniorityFilter.length > 0 || contactFunctionFilter.length > 0 || contactCampaignFilter.length > 0) && (
                  <button onClick={() => { setContactSearch(''); setContactStatusFilter([]); setContactEmailFilter([]); setContactSeniorityFilter([]); setContactFunctionFilter([]); setContactCampaignFilter([]); }}
                    className="text-[10px] text-gray-400 hover:text-gray-600">Clear all</button>
                )}
              </div>
            </div>
            {/* Split panel */}
            <div className="flex gap-4" style={{ minHeight: '70vh' }}>
            {/* Left: Contact list */}
            <div className={`${selectedContactId ? 'w-1/3 min-w-[280px]' : 'w-full'} flex flex-col`}>
              {(() => {
                // Apply filters
                let filtered = [...allContacts];
                if (contactSearch) {
                  const q = contactSearch.toLowerCase();
                  filtered = filtered.filter(c => (c.name || '').toLowerCase().includes(q) || (c.title || '').toLowerCase().includes(q) || (c.company_domain || '').toLowerCase().includes(q) || (c.companies?.name || '').toLowerCase().includes(q));
                }
                if (contactStatusFilter.length > 0) filtered = filtered.filter(c => contactStatusFilter.includes(c.contact_status || 'New'));
                if (contactSeniorityFilter.length > 0) filtered = filtered.filter(c => contactSeniorityFilter.includes(c.seniority));
                if (contactFunctionFilter.length > 0) filtered = filtered.filter(c => contactFunctionFilter.includes(c.function_category));
                if (contactEmailFilter.length > 0) {
                  filtered = filtered.filter(c => {
                    if (contactEmailFilter.includes('has_email') && c.business_email) return true;
                    if (contactEmailFilter.includes('no_email') && !c.business_email) return true;
                    if (contactEmailFilter.includes('enriched') && c.last_enriched_at) return true;
                    return false;
                  });
                }
                if (contactCampaignFilter.length > 0) {
                  filtered = filtered.filter(c => {
                    return allCampaignContacts.some(cc => cc.contact_id === c.id && contactCampaignFilter.includes(cc.campaign_id));
                  });
                }
                // Sort
                filtered.sort((a, b) => {
                  if (contactSort === 'newest') return new Date(b.created_at || 0) - new Date(a.created_at || 0);
                  if (contactSort === 'name') return (a.name || '').localeCompare(b.name || '');
                  if (contactSort === 'company') return (a.company_domain || '').localeCompare(b.company_domain || '');
                  if (contactSort === 'status') {
                    const order = { 'Client': 0, 'Opportunity': 1, 'Engaged': 2, 'New': 3 };
                    return (order[a.contact_status || 'New'] ?? 3) - (order[b.contact_status || 'New'] ?? 3);
                  }
                  if (contactSort === 'last_enriched') return new Date(b.last_enriched_at || 0) - new Date(a.last_enriched_at || 0);
                  if (contactSort === 'last_campaign') return new Date(b.last_campaign_added_at || 0) - new Date(a.last_campaign_added_at || 0);
                  return 0;
                });
                
                const contactStatusColors = { 'New': 'bg-gray-100 text-gray-500', 'Engaged': 'bg-blue-50 text-blue-600', 'Opportunity': 'bg-amber-50 text-amber-600', 'Client': 'bg-green-50 text-green-600' };
                const fmtDate = (d) => d ? new Date(d).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }) : '';
                
                return (
                  <>
                    {filtered.length > 0 && (
                      <div className="flex items-center gap-2 px-4 py-1.5 mb-1 text-xs text-gray-500">
                        <input type="checkbox"
                          checked={filtered.length > 0 && filtered.every(c => checkedContacts.has(c.id))}
                          onChange={(e) => {
                            if (e.target.checked) setCheckedContacts(new Set(filtered.map(c => c.id)));
                            else setCheckedContacts(new Set());
                          }}
                          className="accent-violet-500 cursor-pointer" />
                        <span>Select all ({filtered.length})</span>
                      </div>
                    )}
                    <div className="flex-1 overflow-auto space-y-1">
                      {filtered.length > 0 ? filtered.map(ct => {
                        const isSelected = selectedContactId === ct.id;
                        const isChecked = checkedContacts.has(ct.id);
                        const ctStatus = ct.contact_status || 'New';
                        return (
                          <div key={ct.id}
                            className={`px-4 py-2.5 rounded-lg cursor-pointer transition-colors border ${
                              isChecked ? 'bg-violet-50 border-violet-300' : isSelected ? 'bg-rose-50 border-rose-300' : 'bg-white border-gray-200 hover:border-gray-300'
                            }`}>
                            <div className="flex items-center gap-3">
                              <input type="checkbox" checked={isChecked}
                                onChange={(e) => {
                                  e.stopPropagation();
                                  setCheckedContacts(prev => {
                                    const next = new Set(prev);
                                    if (next.has(ct.id)) next.delete(ct.id); else next.add(ct.id);
                                    return next;
                                  });
                                }}
                                className="accent-violet-500 cursor-pointer flex-shrink-0" />
                              <div className="flex-1 min-w-0" onClick={() => setSelectedContactId(isSelected ? null : ct.id)}>
                                <div className="flex items-center gap-1.5">
                                  <span className="text-sm text-gray-900 font-medium truncate">{ct.name}</span>
                                  <span className={`px-1.5 py-0.5 rounded text-[9px] font-medium ${contactStatusColors[ctStatus] || ''}`}>{ctStatus}</span>
                                </div>
                                <div className="text-xs text-gray-500 truncate">{ct.title}</div>
                              </div>
                              <div className="text-right flex-shrink-0" onClick={() => setSelectedContactId(isSelected ? null : ct.id)}>
                                <div className="text-xs text-gray-500">{ct.companies?.name || ct.company_domain}</div>
                                <div className="text-[10px] text-gray-400">
                                  {ct.seniority}{ct.function_category ? ` · ${ct.function_category}` : ''}
                                  {ct.business_email && <span className="text-emerald-600 ml-1">✉</span>}
                                </div>
                                {ct.created_at && <div className="text-[9px] text-gray-300">{fmtDate(ct.created_at)}</div>}
                              </div>
                            </div>
                          </div>
                        );
                      }) : (
                        <div className="text-center py-20">
                          <div className="text-gray-500 text-lg mb-2">{contactSearch || contactStatusFilter.length > 0 || contactEmailFilter.length > 0 ? 'No matching contacts' : 'No contacts yet'}</div>
                          <div className="text-gray-400 text-sm">{contactSearch || contactStatusFilter.length > 0 ? 'Try adjusting your filters.' : 'Use "Find Contacts" on an account to search via Crustdata.'}</div>
                        </div>
                      )}
                    </div>
                  </>
                );
              })()}
            </div>
            
            {/* Right: Contact detail */}
            {selectedContactId && (() => {
              const ct = allContacts.find(c => c.id === selectedContactId);
              if (!ct) return null;
              const linkedAccount = databaseAccounts.find(c => c.domain === ct.company_domain);
              const fmtDateTime = (d) => d ? new Date(d).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric', hour: 'numeric', minute: '2-digit' }) : 'Never';
              return (
                <div className="flex-1 bg-white border border-gray-200 rounded-lg overflow-auto" style={{ maxHeight: '85vh' }}>
                  <div className="px-5 py-4 border-b border-gray-100">
                    <div className="flex items-center justify-between">
                      <div>
                        <h2 className="text-lg font-bold text-gray-900">{ct.name}</h2>
                        <div className="text-sm text-gray-500 mt-0.5">{ct.title}</div>
                      </div>
                      <div className="flex items-center gap-2">
                        <select value={ct.contact_status || 'New'} onChange={e => updateContactStatus(ct.id, e.target.value)}
                          className="bg-gray-50 border border-gray-200 rounded-lg px-2.5 py-1.5 text-xs text-gray-600 focus:border-violet-300 focus:outline-none">
                          {CONTACT_STATUSES.map(s => <option key={s} value={s}>{s}</option>)}
                        </select>
                        <button onClick={() => setSelectedContactId(null)} className="text-gray-500 hover:text-gray-900 text-lg">×</button>
                      </div>
                    </div>
                  </div>
                  
                  <div className="px-5 py-4 space-y-4">
                    {/* Linked Account */}
                    <div>
                      <div className="text-xs text-gray-500 uppercase tracking-wide font-medium mb-2">Account</div>
                      {linkedAccount ? (
                        <div onClick={() => { setSelectedCompany(companies.indexOf(linkedAccount)); setActiveView('accounts'); }}
                          className="flex items-center gap-3 px-3 py-2.5 bg-gray-100 rounded-lg hover:bg-gray-100 cursor-pointer border border-gray-100">
                          <span className={`inline-block w-8 h-8 rounded-full leading-8 text-center font-bold text-sm ${linkedAccount.totalScore >= 16 ? 'bg-green-100 text-green-600' : linkedAccount.totalScore >= 11 ? 'bg-amber-100 text-amber-600' : 'bg-red-100 text-red-600'}`}>{linkedAccount.totalScore}</span>
                          <div>
                            <div className="text-sm text-gray-900 font-medium">{linkedAccount.companyName}</div>
                            <div className="text-xs text-gray-500">{linkedAccount.domain} · {linkedAccount.icpFit}</div>
                          </div>
                          <span className="ml-auto text-xs text-gray-400">→</span>
                        </div>
                      ) : (
                        <div className="text-xs text-gray-400 px-3 py-2 bg-gray-100 rounded">{ct.company_domain || 'No linked account'}</div>
                      )}
                    </div>
                    
                    {/* Contact details */}
                    <div className="grid grid-cols-2 gap-3">
                      {[
                        { label: 'Seniority', value: ct.seniority },
                        { label: 'Function', value: ct.function_category },
                        { label: 'Region', value: ct.region },
                        { label: 'Experience', value: ct.years_experience ? `${ct.years_experience} years` : '' },
                        { label: 'Email Verified', value: ct.email_verified ? '✓ Yes' : '✗ No' },
                        { label: 'Recent Job Change', value: ct.recent_job_change ? '★ Yes' : 'No' },
                      ].map(({ label, value }) => value ? (
                        <div key={label}>
                          <div className="text-[10px] text-gray-400 uppercase">{label}</div>
                          <div className="text-sm text-gray-400">{value}</div>
                        </div>
                      ) : null)}
                    </div>
                    
                    {/* Activity timeline */}
                    <div>
                      <div className="text-xs text-gray-500 uppercase tracking-wide font-medium mb-2">Activity</div>
                      <div className="space-y-1.5">
                        <div className="flex items-center justify-between text-xs">
                          <span className="text-gray-500">Added</span>
                          <span className="text-gray-700">{fmtDateTime(ct.created_at)}</span>
                        </div>
                        <div className="flex items-center justify-between text-xs">
                          <span className="text-gray-500">Last enriched</span>
                          <span className={ct.last_enriched_at ? 'text-gray-700' : 'text-gray-300'}>{fmtDateTime(ct.last_enriched_at)}</span>
                        </div>
                        <div className="flex items-center justify-between text-xs">
                          <span className="text-gray-500">Last added to campaign</span>
                          <span className={ct.last_campaign_added_at ? 'text-gray-700' : 'text-gray-300'}>{fmtDateTime(ct.last_campaign_added_at)}</span>
                        </div>
                        <div className="flex items-center justify-between text-xs">
                          <span className="text-gray-500">Last updated</span>
                          <span className="text-gray-700">{fmtDateTime(ct.updated_at)}</span>
                        </div>
                      </div>
                    </div>
                    
                    {ct.headline && (
                      <div>
                        <div className="text-[10px] text-gray-400 uppercase mb-1">Headline</div>
                        <div className="text-sm text-gray-500">{ct.headline}</div>
                      </div>
                    )}
                    
                    {/* Business Email */}
                    <div>
                      <div className="text-[10px] text-gray-400 uppercase mb-1">Business Email</div>
                      {ct.business_email ? (
                        <div className="flex items-center gap-2">
                          <a href={`mailto:${ct.business_email}`} className="text-sm text-violet-600 hover:text-violet-700 font-medium">{ct.business_email}</a>
                          <button onClick={() => { navigator.clipboard.writeText(ct.business_email); }} className="text-[10px] text-gray-500 hover:text-gray-700 px-1.5 py-0.5 bg-gray-100 rounded">Copy</button>
                        </div>
                      ) : (
                        <div className="flex items-center gap-2">
                          <span className="text-sm text-gray-400">Not found</span>
                          <button
                            onClick={() => enrichContact(ct)}
                            disabled={enrichingContacts[ct.id] === 'loading'}
                            className={`px-3 py-1 rounded-lg text-xs font-medium transition-colors ${
                              enrichingContacts[ct.id] === 'loading' ? 'bg-violet-100 text-violet-600 cursor-wait' :
                              enrichingContacts[ct.id] === 'error' ? 'bg-red-50 text-red-500 border border-red-200' :
                              'bg-violet-600 hover:bg-violet-500 text-white'
                            }`}
                          >
                            {enrichingContacts[ct.id] === 'loading' ? 'Enriching...' :
                             enrichingContacts[ct.id] === 'error' ? 'Retry Enrich' :
                             'Find Email'}
                          </button>
                        </div>
                      )}
                    </div>
                    
                    <div className="flex gap-2">
                      {ct.linkedin && (
                        <a href={ct.linkedin} target="_blank" rel="noopener noreferrer"
                          className="px-3 py-1.5 bg-gray-100 hover:bg-gray-200 text-gray-700 rounded-lg text-xs font-medium">
                          LinkedIn Profile
                        </a>
                      )}
                      {!ct.business_email && ct.linkedin && enrichingContacts[ct.id] !== 'loading' && (
                        <button onClick={() => enrichContact(ct)}
                          className="px-3 py-1.5 bg-violet-50 hover:bg-violet-100 text-violet-600 rounded-lg text-xs font-medium border border-violet-200">
                          Enrich Email
                        </button>
                      )}
                    </div>
                  </div>
                </div>
              );
            })()}
          </div>
          </div>
        )}


        {/* ======= DISCOVER VIEW ======= */}
        {activeView === 'discover_accounts' && (
          <div>
            {/* Mode toggle + saved filters */}
            <div className="flex items-center gap-3 mb-4">
              <div className="flex items-center gap-1 bg-gray-100 rounded-lg p-1 w-fit">
                <button onClick={() => { setDiscoverMode('indb'); setDiscoverResults([]); setDiscoverTotal(0); setDiscoverError(''); }}
                  className={`px-4 py-1.5 rounded-md text-xs font-medium transition-colors ${discoverMode === 'indb' ? 'bg-white text-gray-900 shadow-sm' : 'text-gray-500 hover:text-gray-700'}`}>
                  In-DB Search
                </button>
                <button onClick={() => { setDiscoverMode('linkedin'); setDiscoverResults([]); setDiscoverTotal(0); setDiscoverError(''); }}
                  className={`px-4 py-1.5 rounded-md text-xs font-medium transition-colors ${discoverMode === 'linkedin' ? 'bg-white text-gray-900 shadow-sm' : 'text-gray-500 hover:text-gray-700'}`}>
                  LinkedIn Search
                </button>
              </div>
              {/* Saved filters */}
              <div className="relative" ref={savedListRef}>
                <button onClick={() => setShowSavedList(!showSavedList)}
                  className="px-3 py-1.5 bg-white hover:bg-gray-50 text-gray-600 border border-gray-200 rounded-lg text-xs font-medium flex items-center gap-1.5">
                  <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M19 21l-7-5-7 5V5a2 2 0 012-2h10a2 2 0 012 2z"/></svg>
                  Saved{savedFilters.length > 0 ? ` (${savedFilters.length})` : ''}
                </button>
                {showSavedList && (
                  <div className="absolute left-0 top-9 z-50 bg-white border border-gray-200 rounded-lg shadow-xl py-1 min-w-[260px] max-h-[350px] overflow-auto">
                    {savedFilters.length === 0 ? (
                      <div className="px-4 py-3 text-xs text-gray-400">No saved filters yet</div>
                    ) : savedFilters.map(preset => (
                      <div key={preset.id} className="flex items-center gap-2 px-3 py-2 hover:bg-gray-50 group">
                        <button onClick={() => handleLoadFilter(preset)} className="flex-1 text-left min-w-0">
                          <div className="text-sm text-gray-700 truncate">{preset.name}</div>
                          <div className="text-[10px] text-gray-400">{preset.mode === 'indb' ? 'In-DB' : 'LinkedIn'} · {(preset.filters || []).length} filters</div>
                        </button>
                        {preset.mode === discoverMode && (
                          <button onClick={() => handleUpdateSavedFilter(preset)} title="Overwrite with current filters"
                            className="text-gray-300 hover:text-violet-500 text-xs opacity-0 group-hover:opacity-100 transition-opacity">↻</button>
                        )}
                        <button onClick={() => handleDeleteSavedFilter(preset.id)} title="Delete"
                          className="text-gray-300 hover:text-red-400 text-xs opacity-0 group-hover:opacity-100 transition-opacity">×</button>
                      </div>
                    ))}
                  </div>
                )}
              </div>
              {/* Save current */}
              {((discoverMode === 'indb' && activeFilters.length > 0) || (discoverMode === 'linkedin' && linkedinFilters.length > 0)) && (
                <div className="relative">
                  {!showSaveDialog ? (
                    <button onClick={() => setShowSaveDialog(true)}
                      className="px-3 py-1.5 bg-white hover:bg-gray-50 text-gray-600 border border-gray-200 rounded-lg text-xs font-medium">
                      Save Filters
                    </button>
                  ) : (
                    <div className="flex items-center gap-2">
                      <input type="text" value={saveFilterName} onChange={e => setSaveFilterName(e.target.value)}
                        placeholder="Preset name..." autoFocus
                        onKeyDown={e => { if (e.key === 'Enter') handleSaveFilters(); if (e.key === 'Escape') setShowSaveDialog(false); }}
                        className="bg-white border border-gray-200 rounded-lg px-3 py-1.5 text-xs text-gray-700 placeholder-gray-400 focus:border-violet-300 focus:outline-none w-40" />
                      <button onClick={handleSaveFilters} disabled={!saveFilterName.trim()}
                        className="px-3 py-1.5 bg-violet-600 hover:bg-violet-500 disabled:bg-violet-600/50 text-white rounded-lg text-xs font-medium">Save</button>
                      <button onClick={() => { setShowSaveDialog(false); setSaveFilterName(''); }}
                        className="text-gray-400 hover:text-gray-600 text-xs">Cancel</button>
                    </div>
                  )}
                </div>
              )}
            </div>

            <div className="bg-white border border-gray-200 rounded-lg p-5 mb-6">
              {/* ===== IN-DB FILTERS ===== */}
              {discoverMode === 'indb' && (
                <>
                  <div className="space-y-3 mb-4">
                    {activeFilters.map(f => {
                      const spec = FILTER_CATALOG.find(s => s.key === f.fieldKey);
                      if (!spec) return null;
                      return (
                        <div key={f.id} className="flex items-start gap-2 bg-gray-50 rounded-lg p-3">
                          <div className="flex items-center gap-2 min-w-[160px] flex-shrink-0 pt-1">
                            <button onClick={() => removeFilter(f.id)} className="text-gray-300 hover:text-red-400 text-sm leading-none">×</button>
                            <span className="text-xs font-medium text-gray-700">{spec.label}</span>
                          </div>
                          {spec.operators.length > 1 && (
                            <select value={f.operator} onChange={e => updateFilter(f.id, { operator: e.target.value })}
                              className="bg-white border border-gray-200 rounded-lg px-2 py-1.5 text-xs text-gray-600 flex-shrink-0 focus:border-violet-300 focus:outline-none">
                              {spec.operators.map(op => <option key={op} value={op}>{OPERATOR_LABELS[op] || op}</option>)}
                            </select>
                          )}
                          <div className="flex-1 min-w-0">
                            {spec.inputType === 'text' && (
                              <input type="text" value={f.value} onChange={e => updateFilter(f.id, { value: e.target.value })}
                                placeholder={`Enter ${spec.label.toLowerCase()}...`}
                                className="w-full bg-white border border-gray-200 rounded-lg px-3 py-1.5 text-sm text-gray-700 placeholder-gray-400 focus:border-violet-300 focus:outline-none" />
                            )}
                            {spec.inputType === 'number' && (
                              <input type="number" value={f.value} onChange={e => updateFilter(f.id, { value: e.target.value })}
                                placeholder="0"
                                className="w-full bg-white border border-gray-200 rounded-lg px-3 py-1.5 text-sm text-gray-700 placeholder-gray-400 focus:border-violet-300 focus:outline-none max-w-[200px]" />
                            )}
                            {spec.inputType === 'date' && (
                              <input type="date" value={f.value} onChange={e => updateFilter(f.id, { value: e.target.value })}
                                className="bg-white border border-gray-200 rounded-lg px-3 py-1.5 text-sm text-gray-700 focus:border-violet-300 focus:outline-none" />
                            )}
                            {spec.inputType === 'select' && (
                              <select value={f.value} onChange={e => updateFilter(f.id, { value: e.target.value })}
                                className="bg-white border border-gray-200 rounded-lg px-3 py-1.5 text-sm text-gray-700 focus:border-violet-300 focus:outline-none">
                                <option value="">Select...</option>
                                {spec.options.map(o => <option key={o} value={o}>{o}</option>)}
                              </select>
                            )}
                            {spec.inputType === 'multi_select' && (
                              <div className="flex flex-wrap gap-1.5">
                                {spec.options.map(opt => (
                                  <button key={opt} onClick={() => {
                                    const arr = Array.isArray(f.value) ? f.value : [];
                                    updateFilter(f.id, { value: arr.includes(opt) ? arr.filter(v => v !== opt) : [...arr, opt] });
                                  }}
                                    className={`px-2.5 py-1 rounded-lg text-xs font-medium transition-colors ${
                                      (Array.isArray(f.value) ? f.value : []).includes(opt) ? 'bg-violet-100 text-violet-600 border border-violet-300' : 'bg-white text-gray-500 border border-gray-200 hover:border-violet-200'
                                    }`}>
                                    {opt}
                                  </button>
                                ))}
                              </div>
                            )}
                            {spec.inputType === 'autocomplete_multi' && (() => {
                              const selectedValues = Array.isArray(f.value) ? f.value : [];
                              const suggestions = autocompleteResults[f.id] || [];
                              return (
                                <div className="relative">
                                  <div className="flex flex-wrap gap-1.5 min-h-[34px] p-1.5 bg-white border border-gray-200 rounded-lg">
                                    {selectedValues.map(v => (
                                      <span key={v} className="inline-flex items-center gap-1 px-2 py-0.5 bg-violet-100 text-violet-600 rounded text-xs">
                                        {v}
                                        <button onClick={() => updateFilter(f.id, { value: selectedValues.filter(x => x !== v) })} className="hover:text-red-500">×</button>
                                      </span>
                                    ))}
                                    <input type="text" placeholder={selectedValues.length > 0 ? 'Add more...' : `Search ${spec.label.toLowerCase()}...`}
                                      onChange={e => fetchAutocomplete(f.id, spec.autocompleteField, e.target.value)}
                                      onFocus={e => { if (e.target.value) fetchAutocomplete(f.id, spec.autocompleteField, e.target.value); }}
                                      className="flex-1 min-w-[100px] bg-transparent border-none outline-none text-sm text-gray-700 placeholder-gray-400 px-1" />
                                  </div>
                                  {suggestions.length > 0 && (
                                    <div className="absolute z-30 left-0 right-0 mt-1 max-h-48 overflow-auto bg-white border border-gray-200 rounded-lg shadow-lg">
                                      {suggestions.filter(s => !selectedValues.includes(s)).map(sug => (
                                        <button key={sug} onClick={() => {
                                          updateFilter(f.id, { value: [...selectedValues, sug] });
                                          setAutocompleteResults(prev => ({ ...prev, [f.id]: [] }));
                                        }}
                                          className="block w-full text-left px-3 py-2 text-sm text-gray-600 hover:bg-violet-50 hover:text-violet-600">
                                          {sug}
                                        </button>
                                      ))}
                                    </div>
                                  )}
                                </div>
                              );
                            })()}
                            {spec.inputType === 'text_list' && (
                              <input type="text" value={Array.isArray(f.value) ? f.value.join(', ') : f.value}
                                onChange={e => updateFilter(f.id, { value: e.target.value.split(',').map(v => v.trim()).filter(Boolean) })}
                                placeholder="value1, value2, value3..."
                                className="w-full bg-white border border-gray-200 rounded-lg px-3 py-1.5 text-sm text-gray-700 placeholder-gray-400 focus:border-violet-300 focus:outline-none" />
                            )}
                            {spec.inputType === 'autocomplete_text' && (() => {
                              const suggestions = autocompleteResults[f.id] || [];
                              return (
                                <div className="relative">
                                  <input type="text" value={f.value || ''} onChange={e => {
                                    updateFilter(f.id, { value: e.target.value });
                                    if (spec.autocompleteField) fetchAutocomplete(f.id, spec.autocompleteField, e.target.value);
                                  }}
                                    onBlur={() => setTimeout(() => setAutocompleteResults(prev => ({ ...prev, [f.id]: [] })), 200)}
                                    placeholder={`Search ${spec.label.toLowerCase()}...`}
                                    className="w-full bg-white border border-gray-200 rounded-lg px-3 py-1.5 text-sm text-gray-700 placeholder-gray-400 focus:border-violet-300 focus:outline-none" />
                                  {suggestions.length > 0 && (
                                    <div className="absolute z-30 left-0 right-0 mt-1 max-h-48 overflow-auto bg-white border border-gray-200 rounded-lg shadow-lg">
                                      {suggestions.map(sug => (
                                        <button key={sug} onMouseDown={e => e.preventDefault()} onClick={() => {
                                          updateFilter(f.id, { value: sug });
                                          setAutocompleteResults(prev => ({ ...prev, [f.id]: [] }));
                                        }}
                                          className="block w-full text-left px-3 py-2 text-sm text-gray-600 hover:bg-violet-50 hover:text-violet-600 truncate">
                                          {sug}
                                        </button>
                                      ))}
                                    </div>
                                  )}
                                </div>
                              );
                            })()}
                          </div>
                        </div>
                      );
                    })}
                  </div>
                  <div className="flex items-center gap-3 flex-wrap">
                    <div className="relative" ref={filterPickerRef}>
                      <button onClick={() => setShowFilterPicker(!showFilterPicker)}
                        className="px-3 py-1.5 bg-gray-50 hover:bg-gray-100 text-gray-600 border border-gray-200 rounded-lg text-xs font-medium flex items-center gap-1.5">
                        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
                        Add Filter
                      </button>
                      {showFilterPicker && (
                        <div className="absolute left-0 top-9 z-50 bg-white border border-gray-200 rounded-lg shadow-xl py-1 min-w-[280px] max-h-[400px] overflow-auto">
                          <div className="px-3 py-2 sticky top-0 bg-white border-b border-gray-100">
                            <input type="text" placeholder="Search filters..." value={filterPickerSearch} onChange={e => setFilterPickerSearch(e.target.value)} autoFocus
                              className="w-full bg-gray-50 border border-gray-200 rounded-lg px-3 py-1.5 text-xs text-gray-700 placeholder-gray-400 focus:border-violet-300 focus:outline-none" />
                          </div>
                          {(() => {
                            const q = filterPickerSearch.toLowerCase();
                            const filtered = FILTER_CATALOG.filter(f => f.label.toLowerCase().includes(q) || f.category.toLowerCase().includes(q) || f.key.toLowerCase().includes(q));
                            const categories = [...new Set(filtered.map(f => f.category))];
                            return categories.map(cat => (
                              <div key={cat}>
                                <div className="px-3 py-1.5 text-[10px] text-gray-400 uppercase tracking-wide font-medium bg-gray-50">{cat}</div>
                                {filtered.filter(f => f.category === cat).map(f => (
                                  <button key={f.key} onClick={() => addFilter(f.key)}
                                    className="block w-full text-left px-4 py-2 text-[13px] text-gray-600 hover:bg-violet-50 hover:text-violet-600">
                                    {f.label}
                                    <span className="text-[10px] text-gray-400 ml-2">{f.key}</span>
                                  </button>
                                ))}
                              </div>
                            ));
                          })()}
                        </div>
                      )}
                    </div>
                    <button onClick={() => runDiscovery(false)} disabled={discoverLoading || activeFilters.length === 0}
                      className="px-5 py-2 bg-violet-600 hover:bg-violet-500 disabled:bg-violet-600/50 text-white rounded-lg text-sm font-medium">
                      {discoverLoading ? 'Searching...' : 'Search Companies'}
                    </button>
                    <select value={discoverApiSort} onChange={e => setDiscoverApiSort(e.target.value)}
                      className="bg-gray-50 border border-gray-200 rounded-lg px-3 py-2 text-xs text-gray-600 focus:border-violet-300 focus:outline-none">
                      {DISCOVER_API_SORTS.map(s => (
                        <option key={s.key} value={s.key}>{s.label}</option>
                      ))}
                    </select>
                    {activeFilters.length > 0 && (
                      <button onClick={() => setActiveFilters([])} className="text-xs text-gray-400 hover:text-gray-600">Clear all</button>
                    )}
                    {discoverTotal > 0 && <span className="text-xs text-gray-500 ml-auto">{discoverTotal.toLocaleString()} total matches</span>}
                  </div>
                </>
              )}

              {/* ===== LINKEDIN FILTERS ===== */}
              {discoverMode === 'linkedin' && (
                <>
                  <div className="space-y-3 mb-4">
                    {linkedinFilters.map(f => {
                      const spec = LINKEDIN_FILTER_CATALOG.find(s => s.key === f.filterKey);
                      if (!spec) return null;
                      return (
                        <div key={f.id} className="flex items-start gap-2 bg-gray-50 rounded-lg p-3">
                          <div className="flex items-center gap-2 min-w-[160px] flex-shrink-0 pt-1">
                            <button onClick={() => removeLinkedinFilter(f.id)} className="text-gray-300 hover:text-red-400 text-sm leading-none">×</button>
                            <span className="text-xs font-medium text-gray-700">{spec.label}</span>
                          </div>
                          <div className="flex-1 min-w-0">
                            {/* Boolean — no UI needed, just the label */}
                            {spec.filterKind === 'boolean' && (
                              <span className="text-xs text-green-600 bg-green-50 px-2 py-1 rounded">Enabled</span>
                            )}
                            {/* Keyword — single text input */}
                            {spec.filterKind === 'keyword' && (
                              <input type="text" value={f.value || ''} onChange={e => updateLinkedinFilter(f.id, { value: e.target.value })}
                                placeholder="Enter keyword..."
                                className="w-full bg-white border border-gray-200 rounded-lg px-3 py-1.5 text-sm text-gray-700 placeholder-gray-400 focus:border-violet-300 focus:outline-none max-w-[300px]" />
                            )}
                            {/* Text — multi-select pills or autocomplete */}
                            {spec.filterKind === 'text' && !spec.usesAutocomplete && spec.options && (
                              <div>
                                {spec.supportsNotIn && (
                                  <label className="flex items-center gap-1.5 mb-2 text-xs text-gray-500 cursor-pointer">
                                    <input type="checkbox" checked={f.notIn || false} onChange={e => updateLinkedinFilter(f.id, { notIn: e.target.checked })} className="accent-violet-500" />
                                    Exclude (not in)
                                  </label>
                                )}
                                <div className="flex flex-wrap gap-1.5">
                                  {spec.options.map(opt => (
                                    <button key={opt} onClick={() => {
                                      const arr = Array.isArray(f.value) ? f.value : [];
                                      updateLinkedinFilter(f.id, { value: arr.includes(opt) ? arr.filter(v => v !== opt) : [...arr, opt] });
                                    }}
                                      className={`px-2.5 py-1 rounded-lg text-xs font-medium transition-colors ${
                                        (Array.isArray(f.value) ? f.value : []).includes(opt) ? 'bg-violet-100 text-violet-600 border border-violet-300' : 'bg-white text-gray-500 border border-gray-200 hover:border-violet-200'
                                      }`}>
                                      {opt}
                                    </button>
                                  ))}
                                </div>
                              </div>
                            )}
                            {/* Text with autocomplete (REGION, INDUSTRY) */}
                            {spec.filterKind === 'text' && spec.usesAutocomplete && (() => {
                              const selectedValues = Array.isArray(f.value) ? f.value : [];
                              const suggestions = autocompleteResults[`li_${f.id}`] || [];
                              return (
                                <div>
                                  {spec.supportsNotIn && (
                                    <label className="flex items-center gap-1.5 mb-2 text-xs text-gray-500 cursor-pointer">
                                      <input type="checkbox" checked={f.notIn || false} onChange={e => updateLinkedinFilter(f.id, { notIn: e.target.checked })} className="accent-violet-500" />
                                      Exclude (not in)
                                    </label>
                                  )}
                                  <div className="relative">
                                    <div className="flex flex-wrap gap-1.5 min-h-[34px] p-1.5 bg-white border border-gray-200 rounded-lg">
                                      {selectedValues.map(v => (
                                        <span key={v} className="inline-flex items-center gap-1 px-2 py-0.5 bg-violet-100 text-violet-600 rounded text-xs">
                                          {v}
                                          <button onClick={() => updateLinkedinFilter(f.id, { value: selectedValues.filter(x => x !== v) })} className="hover:text-red-500">×</button>
                                        </span>
                                      ))}
                                      <input type="text" placeholder={selectedValues.length > 0 ? 'Add more...' : `Search ${spec.label.toLowerCase()}...`}
                                        onChange={e => fetchLinkedinAutocomplete(f.id, spec.usesAutocomplete, e.target.value)}
                                        onFocus={e => { if (e.target.value) fetchLinkedinAutocomplete(f.id, spec.usesAutocomplete, e.target.value); }}
                                        className="flex-1 min-w-[100px] bg-transparent border-none outline-none text-sm text-gray-700 placeholder-gray-400 px-1" />
                                    </div>
                                    {suggestions.length > 0 && (
                                      <div className="absolute z-30 left-0 right-0 mt-1 max-h-48 overflow-auto bg-white border border-gray-200 rounded-lg shadow-lg">
                                        {suggestions.filter(s => !selectedValues.includes(s)).map(sug => (
                                          <button key={sug} onClick={() => {
                                            updateLinkedinFilter(f.id, { value: [...selectedValues, sug] });
                                            setAutocompleteResults(prev => ({ ...prev, [`li_${f.id}`]: [] }));
                                          }}
                                            className="block w-full text-left px-3 py-2 text-sm text-gray-600 hover:bg-violet-50 hover:text-violet-600">
                                            {sug}
                                          </button>
                                        ))}
                                      </div>
                                    )}
                                  </div>
                                </div>
                              );
                            })()}
                            {/* Range — min/max + optional sub_filter */}
                            {spec.filterKind === 'range' && (
                              <div className="flex items-center gap-2 flex-wrap">
                                {spec.subFilterOptions && (
                                  <select value={f.subFilter || ''} onChange={e => updateLinkedinFilter(f.id, { subFilter: e.target.value })}
                                    className="bg-white border border-gray-200 rounded-lg px-2 py-1.5 text-xs text-gray-600 focus:border-violet-300 focus:outline-none">
                                    {spec.subFilterOptions.map(o => <option key={o} value={o}>{o}</option>)}
                                  </select>
                                )}
                                <input type="number" value={f.min || ''} onChange={e => updateLinkedinFilter(f.id, { min: e.target.value })}
                                  placeholder="Min" className="w-24 bg-white border border-gray-200 rounded-lg px-3 py-1.5 text-sm text-gray-700 placeholder-gray-400 focus:border-violet-300 focus:outline-none" />
                                <span className="text-xs text-gray-400">to</span>
                                <input type="number" value={f.max || ''} onChange={e => updateLinkedinFilter(f.id, { max: e.target.value })}
                                  placeholder="Max" className="w-24 bg-white border border-gray-200 rounded-lg px-3 py-1.5 text-sm text-gray-700 placeholder-gray-400 focus:border-violet-300 focus:outline-none" />
                              </div>
                            )}
                          </div>
                        </div>
                      );
                    })}
                  </div>
                  <div className="flex items-center gap-3 flex-wrap">
                    <div className="relative" ref={filterPickerRef}>
                      <button onClick={() => setShowFilterPicker(!showFilterPicker)}
                        className="px-3 py-1.5 bg-gray-50 hover:bg-gray-100 text-gray-600 border border-gray-200 rounded-lg text-xs font-medium flex items-center gap-1.5">
                        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
                        Add Filter
                      </button>
                      {showFilterPicker && (
                        <div className="absolute left-0 top-9 z-50 bg-white border border-gray-200 rounded-lg shadow-xl py-1 min-w-[280px] max-h-[400px] overflow-auto">
                          <div className="px-3 py-2 sticky top-0 bg-white border-b border-gray-100">
                            <input type="text" placeholder="Search filters..." value={filterPickerSearch} onChange={e => setFilterPickerSearch(e.target.value)} autoFocus
                              className="w-full bg-gray-50 border border-gray-200 rounded-lg px-3 py-1.5 text-xs text-gray-700 placeholder-gray-400 focus:border-violet-300 focus:outline-none" />
                          </div>
                          {(() => {
                            const q = filterPickerSearch.toLowerCase();
                            const filtered = LINKEDIN_FILTER_CATALOG.filter(f => f.label.toLowerCase().includes(q) || f.category.toLowerCase().includes(q) || f.key.toLowerCase().includes(q));
                            const categories = [...new Set(filtered.map(f => f.category))];
                            return categories.map(cat => (
                              <div key={cat}>
                                <div className="px-3 py-1.5 text-[10px] text-gray-400 uppercase tracking-wide font-medium bg-gray-50">{cat}</div>
                                {filtered.filter(f => f.category === cat).map(f => (
                                  <button key={f.key} onClick={() => addLinkedinFilter(f.key)}
                                    className="block w-full text-left px-4 py-2 text-[13px] text-gray-600 hover:bg-violet-50 hover:text-violet-600">
                                    {f.label}
                                  </button>
                                ))}
                              </div>
                            ));
                          })()}
                        </div>
                      )}
                    </div>
                    <button onClick={() => runDiscovery(false)} disabled={discoverLoading || linkedinFilters.length === 0}
                      className="px-5 py-2 bg-violet-600 hover:bg-violet-500 disabled:bg-violet-600/50 text-white rounded-lg text-sm font-medium">
                      {discoverLoading ? 'Searching...' : 'Search Companies'}
                    </button>
                    {linkedinFilters.length > 0 && (
                      <button onClick={() => setLinkedinFilters([])} className="text-xs text-gray-400 hover:text-gray-600">Clear all</button>
                    )}
                    {discoverTotal > 0 && <span className="text-xs text-gray-500 ml-auto">{discoverTotal.toLocaleString()} total matches</span>}
                  </div>
                </>
              )}
              {discoverError && <div className="mt-3 text-sm text-red-600">{discoverError}</div>}
            </div>

            {/* Shared results section */}
            {discoverResults.length > 0 && (
              <div>
                <div className="flex items-center gap-3 mb-3">
                  <button onClick={() => addDiscoveredToQueue()} className="px-4 py-1.5 bg-violet-600 hover:bg-violet-500 text-white rounded-lg text-sm font-medium">
                    Screen All {discoverResults.length}
                  </button>
                  {discoverSelected.size > 0 && (
                    <button onClick={() => addDiscoveredToQueue(discoverSelected)} className="px-4 py-1.5 bg-white hover:bg-gray-50 text-violet-600 border border-violet-200 rounded-lg text-sm font-medium">
                      Screen {discoverSelected.size} Selected
                    </button>
                  )}
                  {/* Column picker */}
                  <div className="relative" ref={columnPickerRef}>
                    <button onClick={() => setShowColumnPicker(!showColumnPicker)}
                      className="px-3 py-1.5 bg-gray-50 hover:bg-gray-100 text-gray-500 border border-gray-200 rounded-lg text-xs font-medium">
                      Columns ({discoverColumns.length})
                    </button>
                    {showColumnPicker && (
                      <div className="absolute left-0 top-9 z-50 bg-white border border-gray-200 rounded-lg shadow-xl py-1 min-w-[240px] max-h-[400px] overflow-auto">
                        <div className="px-3 py-1.5 flex items-center justify-between border-b border-gray-100">
                          <span className="text-[10px] text-gray-400 uppercase tracking-wide">Toggle Columns</span>
                          <button onClick={() => setDiscoverColumns([...DEFAULT_COLUMNS])} className="text-[10px] text-violet-600 hover:text-violet-700">Reset</button>
                        </div>
                        {ALL_DISCOVER_COLUMNS.map(col => (
                          <label key={col.key} className="flex items-center gap-2 px-3 py-1.5 hover:bg-gray-50 cursor-pointer">
                            <input type="checkbox"
                              checked={discoverColumns.includes(col.key)}
                              onChange={() => setDiscoverColumns(prev =>
                                prev.includes(col.key) ? prev.filter(k => k !== col.key) : [...prev, col.key]
                              )}
                              className="accent-violet-500 w-3 h-3" />
                            <span className="text-xs text-gray-600">{col.label}</span>
                          </label>
                        ))}
                      </div>
                    )}
                  </div>
                  <span className="text-xs text-gray-500 ml-auto">{discoverResults.length} results · {discoverTotal.toLocaleString()} total matches</span>
                </div>
                {(() => {
                  const activeCols = discoverColumns.map(k => ALL_DISCOVER_COLUMNS.find(c => c.key === k)).filter(Boolean);
                  const totalW = 1 + activeCols.reduce((s, c) => s + c.width, 0); // 1 for checkbox
                  const fmtCell = (co, col) => {
                    const v = co[col.key];
                    if (col.key === 'domain') return null; // handled specially
                    if (col.type === 'number') return v ? v.toLocaleString() : '-';
                    if (col.type === 'money') return v ? (v >= 1e9 ? `$${(v/1e9).toFixed(1)}B` : v >= 1e6 ? `$${(v/1e6).toFixed(0)}M` : v >= 1e3 ? `$${(v/1e3).toFixed(0)}K` : `$${v}`) : '-';
                    if (col.type === 'percent') return v ? `${v > 0 ? '+' : ''}${v.toFixed(1)}%` : '-';
                    if (col.type === 'date') return v ? new Date(v).toLocaleDateString('en-US', { year: 'numeric', month: 'short' }) : '-';
                    return v || '-';
                  };
                  // Sort results
                  const sorted = [...discoverResults].sort((a, b) => {
                    const col = ALL_DISCOVER_COLUMNS.find(c => c.key === discoverSort.key);
                    const av = a[discoverSort.key], bv = b[discoverSort.key];
                    if (col?.type === 'number' || col?.type === 'money' || col?.type === 'percent') {
                      const diff = (Number(av) || 0) - (Number(bv) || 0);
                      return discoverSort.dir === 'asc' ? diff : -diff;
                    }
                    const diff = String(av || '').localeCompare(String(bv || ''));
                    return discoverSort.dir === 'asc' ? diff : -diff;
                  });
                  return (
                    <div className="bg-white border border-gray-200 rounded-lg overflow-x-auto">
                      <table className="w-full text-left">
                        <thead>
                          <tr className="border-b border-gray-100">
                            <th className="px-3 py-2 w-8"></th>
                            {activeCols.map(col => (
                              <th key={col.key}
                                onClick={() => setDiscoverSort(prev => prev.key === col.key ? { key: col.key, dir: prev.dir === 'asc' ? 'desc' : 'asc' } : { key: col.key, dir: col.align === 'right' ? 'desc' : 'asc' })}
                                className={`px-2 py-2 text-[10px] text-gray-400 uppercase tracking-wide font-medium cursor-pointer hover:text-gray-600 select-none whitespace-nowrap ${col.align === 'right' ? 'text-right' : ''}`}>
                                {col.label}
                                {discoverSort.key === col.key && <span className="ml-0.5">{discoverSort.dir === 'asc' ? '↑' : '↓'}</span>}
                              </th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {sorted.map(co => (
                            <tr key={co.domain} onClick={() => toggleDiscoverSelect(co.domain)}
                              className={`border-b border-gray-100 last:border-0 cursor-pointer transition-colors ${discoverSelected.has(co.domain) ? 'bg-violet-50' : 'hover:bg-gray-50'}`}>
                              <td className="px-3 py-2">
                                <div className={`w-4 h-4 rounded border flex items-center justify-center text-[10px] ${discoverSelected.has(co.domain) ? 'bg-violet-500 border-violet-500 text-white' : 'border-gray-300'}`}>{discoverSelected.has(co.domain) && '✓'}</div>
                              </td>
                              {activeCols.map(col => (
                                <td key={col.key} className={`px-2 py-2 max-w-[200px] truncate ${col.align === 'right' ? 'text-right' : ''} ${col.key === 'name' ? 'text-sm text-gray-900 font-medium' : 'text-[11px] text-gray-500'}`}
                                  title={typeof co[col.key] === 'string' ? co[col.key] : undefined}>
                                  {col.key === 'domain' ? (
                                    <a href={co.website || `https://${co.domain}`} target="_blank" rel="noopener noreferrer"
                                      onClick={e => e.stopPropagation()}
                                      className="text-violet-600 hover:text-violet-700 hover:underline">{co.domain}</a>
                                  ) : fmtCell(co, col)}
                                </td>
                              ))}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  );
                })()}
                {(discoverCursor || (discoverMode === 'linkedin' && discoverResults.length < discoverTotal)) && (
                  <div className="mt-4 text-center">
                    <button onClick={() => runDiscovery(true)} disabled={discoverLoading}
                      className="px-5 py-2 bg-gray-100 hover:bg-gray-200 border border-gray-200 rounded-lg text-sm text-gray-400">
                      {discoverLoading ? 'Loading...' : 'Load More'}
                    </button>
                  </div>
                )}
              </div>
            )}

            {!discoverLoading && discoverResults.length === 0 && !discoverError && (
              <div className="text-center py-16">
                <div className="text-gray-500 text-lg mb-2">Discover B2B SaaS companies via Crustdata</div>
                <div className="text-gray-400 text-sm">{discoverMode === 'indb' ? 'In-DB Search: Precise numeric filters on Crustdata\'s enriched company database.' : 'LinkedIn Search: LinkedIn signals like hiring, leadership changes, department size, and Fortune ranking.'}</div>
              </div>
            )}
          </div>
        )}

        {/* ======= UPLOAD & RUN VIEW ======= */}
        {activeView === 'upload' && (
          <div>
            <div className="flex items-center gap-4 mb-6 flex-wrap">
              <input type="file" ref={fileInputRef} onChange={handleFileUpload} accept=".csv" className="hidden" />
              <button onClick={() => fileInputRef.current?.click()} className="px-4 py-2 bg-violet-600 hover:bg-violet-500 text-white rounded-lg text-sm font-medium">Upload CSV</button>
              {pendingCompanies.length > 0 && !isProcessing && (
                <button onClick={processAll} className="px-4 py-2 bg-violet-600 hover:bg-violet-500 text-white rounded-lg text-sm font-medium">
                  Screen {pendingCompanies.length} Companies
                </button>
              )}
              {isProcessing && <button onClick={stopProcessing} className="px-4 py-2 text-red-500 hover:text-red-700 border border-red-200 hover:bg-red-50 rounded-lg text-sm font-medium">Stop</button>}
              <div className="flex items-center gap-2 ml-auto">
                <label className="text-xs text-gray-500">Concurrency:</label>
                <select value={concurrency} onChange={e => setConcurrency(parseInt(e.target.value))} className="bg-white border border-gray-200 rounded-lg px-2 py-1 text-xs text-gray-600">
                  {[1,2,3,4,5].map(n => <option key={n} value={n}>{n}</option>)}
                </select>
              </div>
            </div>

            {pendingCompanies.length > 0 && (
              <div className="mb-4 flex gap-4 text-sm">
                <span className="text-gray-500">{pendingCompanies.length} pending</span>
                {errorCount > 0 && <span className="text-red-600">{errorCount} errors</span>}
              </div>
            )}

            {pendingCompanies.length > 0 ? (
              <div className="space-y-3">
                {pendingCompanies.map((c) => {
                  const origIdx = companies.indexOf(c);
                  return (
                    <div key={c.domain || origIdx} className="bg-white border border-gray-200 rounded-lg overflow-hidden">
                      <div className="flex items-center gap-4 px-5 py-3">
                        <div className="flex-shrink-0">
                          <span className="inline-block w-10 h-10 rounded-full leading-10 text-center text-gray-400 bg-gray-100">-</span>
                        </div>
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-3">
                            <span className="font-medium text-gray-900">{c.companyName}</span>
                            <span className="text-xs text-gray-500">{c.website}</span>
                          </div>
                        </div>
                        <div className="w-32 text-right">
                          {c.status === 'pending' && <span className="text-gray-500 text-xs">Pending</span>}
                          {c.status === 'processing' && <span className="text-violet-600 animate-pulse text-xs">{c.step || 'Processing...'}</span>}
                          {c.status === 'error' && <span className="text-red-600 text-xs" title={c.error}>Error — will retry</span>}
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>
            ) : (
              <div className="text-center py-20">
                <div className="text-gray-500 text-lg mb-2">Upload a CSV with company URLs to start screening</div>
                <div className="text-gray-400 text-sm">CSV needs a Website/URL column. Company Name column is optional.</div>
                <div className="text-gray-400 text-sm mt-1">Already-screened accounts will be skipped.</div>
              </div>
            )}
          </div>
        )}

        {/* ======= CAMPAIGNS VIEW ======= */}
        {activeView === 'campaigns' && (
          <div>
            {/* Campaign header bar */}
            <div className="flex items-center gap-3 mb-4">
              <select
                value={activeCampaignId || ''}
                onChange={e => setActiveCampaignId(e.target.value || null)}
                className="bg-white border border-gray-200 rounded-lg px-3 py-1.5 text-[13px] text-gray-700 min-w-[180px]"
              >
                <option value="">Select campaign...</option>
                {campaigns.map(c => (
                  <option key={c.id} value={c.id}>{c.name} ({c.status})</option>
                ))}
              </select>
              <button onClick={handleCreateCampaign} className="px-3 py-1.5 bg-violet-600 hover:bg-violet-500 rounded-lg text-[13px] font-medium text-white">+ New Campaign</button>
              {activeCampaignId && (
                <>
                  <div className="relative">
                    <button
                      onClick={() => { setInstantlyPicker(!instantlyPicker); if (!instantlyPicker) fetchInstantlyCampaigns(); }}
                      disabled={instantlyLoading || campaignContacts.length === 0}
                      className="px-3 py-1.5 bg-white hover:bg-gray-50 text-gray-700 border border-gray-200 rounded-lg text-[13px] font-medium disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-1.5"
                    >
                      {instantlyLoading ? (
                        <span className="animate-pulse">Pushing...</span>
                      ) : (
                        <>
                          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M22 2L11 13"/><path d="M22 2L15 22L11 13L2 9L22 2Z"/></svg>
                          Push to Instantly
                        </>
                      )}
                    </button>
                    {instantlyPicker && (
                      <div className="absolute left-0 top-9 z-50 bg-white border border-gray-200 rounded-lg shadow-xl py-1 min-w-[280px]">
                        <div className="px-3 py-2 text-[10px] text-gray-400 uppercase tracking-wide border-b border-gray-100">Select Instantly Campaign</div>
                        {instantlyCampaigns.length > 0 ? instantlyCampaigns.map(ic => (
                          <button key={ic.id} onClick={() => pushToInstantly(ic.id)}
                            className="block w-full text-left px-4 py-2 text-[13px] text-gray-600 hover:bg-violet-50 hover:text-violet-600 truncate">
                            {ic.name}
                          </button>
                        )) : (
                          <div className="px-4 py-3 text-xs text-gray-400">
                            {instantlyCampaigns.length === 0 ? 'Loading campaigns...' : 'No active Instantly campaigns found.'}
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                  {instantlyResult && (
                    <span className={`text-xs ${instantlyResult.type === 'success' ? 'text-green-600' : 'text-red-500'}`}>
                      {instantlyResult.message}
                    </span>
                  )}
                  <button onClick={handleDeleteCampaign} className="px-3 py-1.5 text-red-500 hover:text-red-700 border border-red-200 hover:bg-red-50 rounded-lg text-[13px] ml-auto">🗑 Delete Campaign</button>
                </>
              )}
            </div>

            {!activeCampaignId ? (
              <div className="text-center py-20">
                <div className="text-gray-500 text-base mb-2">Select or create a campaign</div>
                <div className="text-gray-400 text-[13px]">Campaigns let you group contacts and draft outreach sequences.</div>
              </div>
            ) : (
              <div className="bg-white border border-gray-200 rounded-xl overflow-hidden" style={{ height: '80vh' }}>
                <div className="flex h-full">
                {/* LEFT: Campaign contacts list */}
                <div className="flex flex-col border-r border-gray-200 overflow-hidden" style={{ width: campPanelWidths[0], minWidth: 160, flexShrink: 0 }}>
                  <div className="px-4 py-3 border-b border-gray-100">
                    <div className="text-[11px] text-gray-400 uppercase tracking-wide font-medium">Campaign Contacts ({campaignContacts.length})</div>
                  </div>
                  <div className="flex-1 overflow-auto p-2 space-y-0.5">
                    {campaignContacts.map(cc => {
                      const ct = cc.contacts;
                      if (!ct) return null;
                      const isSelected = selectedCampaignContact === cc.contact_id;
                      return (
                        <div key={cc.id}
                          onClick={() => setSelectedCampaignContact(cc.contact_id)}
                          className={`flex items-center gap-2 px-3 py-2 rounded-lg cursor-pointer transition-colors ${
                            isSelected ? 'bg-violet-50' : 'hover:bg-gray-50'
                          }`}>
                          <span className={`w-7 h-7 rounded-full flex items-center justify-center text-[11px] font-bold flex-shrink-0 ${
                            isSelected ? 'bg-violet-200 text-violet-700' : 'bg-gray-100 text-gray-500'
                          }`}>
                            {(ct.name || '?')[0].toUpperCase()}
                          </span>
                          <div className="flex-1 min-w-0">
                            <div className="text-[13px] text-gray-900 font-medium truncate">{ct.name}</div>
                            <div className="text-[10px] text-gray-400 truncate">{ct.title}</div>
                          </div>
                          <div className="flex items-center gap-1 flex-shrink-0">
                            {ct.business_email && <span className="w-2 h-2 rounded-full bg-emerald-400" title="Has email" />}
                            {genContactsWithMessages.has(cc.contact_id) && <span className="w-2 h-2 rounded-full bg-violet-400" title="Messages generated" />}
                          </div>
                        </div>
                      );
                    })}
                    {campaignContacts.length === 0 && (
                      <div className="text-center py-10 text-gray-400 text-sm">No contacts yet. Select contacts in the Contacts tab and use "+ Campaign" to add them.</div>
                    )}
                  </div>
                </div>

                {/* DRAG HANDLE 1: between left and middle */}
                <div className="w-1 cursor-col-resize bg-gray-200 hover:bg-violet-300 active:bg-violet-400 transition-colors flex-shrink-0"
                  onMouseDown={(e) => {
                    e.preventDefault();
                    const startX = e.clientX;
                    const startW = campPanelWidths[0];
                    const onMove = (ev) => {
                      const delta = ev.clientX - startX;
                      setCampPanelWidths(prev => [Math.max(160, Math.min(400, startW + delta)), prev[1], prev[2]]);
                    };
                    const onUp = () => { document.removeEventListener('mousemove', onMove); document.removeEventListener('mouseup', onUp); };
                    document.addEventListener('mousemove', onMove);
                    document.addEventListener('mouseup', onUp);
                  }} />

                {/* MIDDLE: Selected contact + full account details */}
                <div className="flex flex-col border-r border-gray-200 overflow-hidden" style={{ width: campPanelWidths[1], minWidth: 200, flexShrink: 0 }}>
                  {selectedCampaignContact ? (() => {
                    const cc = campaignContacts.find(c => c.contact_id === selectedCampaignContact);
                    const ct = cc?.contacts;
                    if (!ct) return <div className="text-gray-400 text-[13px] p-4">Contact not found</div>;
                    const companyBasic = ct.companies;
                    const fullAccount = companyBasic?.domain ? companies.find(c => c.domain === companyBasic.domain && c.status === 'complete') : null;
                    const accountContacts = companyBasic?.domain ? allContacts.filter(ac => ac.company_domain === companyBasic.domain) : [];
                    return (
                      <div className="flex-1 overflow-y-auto">
                        {/* Contact header */}
                        <div className="px-5 py-4 border-b border-gray-100 sticky top-0 bg-white/95 backdrop-blur z-10">
                          <div className="flex items-center gap-3">
                            <span className="w-10 h-10 rounded-full bg-violet-100 flex items-center justify-center text-base font-bold text-violet-600">
                              {(ct.name || '?')[0].toUpperCase()}
                            </span>
                            <div className="flex-1 min-w-0">
                              <h3 className="text-[15px] font-bold text-gray-900">{ct.name}</h3>
                              <div className="text-[11px] text-gray-500">{ct.title}</div>
                            </div>
                          </div>
                        </div>
                        <div className="px-5 py-3 space-y-2.5 border-b border-gray-100">
                          {/* Contact details */}
                          <div className="grid grid-cols-2 gap-x-3 gap-y-1.5 text-[11px]">
                            {companyBasic && (
                              <div className="col-span-2">
                                <span className="text-gray-400">Company:</span>{' '}
                                <span className="text-gray-900 font-semibold">{companyBasic.name}</span>
                                {companyBasic.domain && <> · <a href={`https://${companyBasic.domain}`} target="_blank" rel="noopener noreferrer" className="text-violet-600 hover:text-violet-700">{companyBasic.domain}</a></>}
                              </div>
                            )}
                            <div>
                              <span className="text-gray-400">Email:</span>{' '}
                              {ct.business_email ? (
                                <a href={`mailto:${ct.business_email}`} className="text-violet-600 hover:text-violet-700">{ct.business_email}</a>
                              ) : (
                                <span className="text-amber-600">None</span>
                              )}
                              {!ct.business_email && ct.linkedin && (
                                <button onClick={() => enrichContact(ct)} disabled={enrichingContacts[ct.id] === 'loading'}
                                  className="ml-1 px-1.5 py-0.5 bg-violet-50 text-violet-600 rounded text-[9px] border border-violet-200">
                                  {enrichingContacts[ct.id] === 'loading' ? '...' : 'Find'}
                                </button>
                              )}
                            </div>
                            {ct.linkedin && (
                              <div>
                                <span className="text-gray-400">LinkedIn:</span>{' '}
                                <a href={ct.linkedin} target="_blank" rel="noopener noreferrer" className="text-sky-600 hover:text-sky-700">
                                  {ct.linkedin.replace('https://www.linkedin.com/in/', '').replace(/\/$/, '')}
                                </a>
                              </div>
                            )}
                            {ct.seniority && <div><span className="text-gray-400">Seniority:</span> <span className="text-gray-600">{ct.seniority}</span></div>}
                            {ct.function_category && <div><span className="text-gray-400">Function:</span> <span className="text-gray-600">{ct.function_category}</span></div>}
                            {ct.region && <div><span className="text-gray-400">Region:</span> <span className="text-gray-600">{ct.region}</span></div>}
                            {ct.years_experience > 0 && <div><span className="text-gray-400">Experience:</span> <span className="text-gray-600">{ct.years_experience}y</span></div>}
                          </div>
                        </div>

                        {/* === FULL ACCOUNT DETAILS (mirrors Accounts tab) === */}
                        {fullAccount ? (
                          <>
                            {/* Account header */}
                            <div className="px-5 py-3 border-b border-gray-100">
                              <div className="flex items-center gap-2 mb-1">
                                <span className="text-[9px] text-gray-500 uppercase tracking-wider font-semibold">Account</span>
                                <span className={`font-bold text-sm ${fullAccount.totalScore >= 16 ? 'text-green-600' : fullAccount.totalScore >= 11 ? 'text-amber-600' : 'text-red-600'}`}>{fullAccount.totalScore}/21</span>
                                {fullAccount.icpFit && <span className={`px-1.5 py-0.5 rounded text-[10px] border ${
                                  fullAccount.icpFit === 'Strong' ? 'bg-green-50 text-green-700 border-green-200' :
                                  fullAccount.icpFit === 'Moderate' ? 'bg-amber-50 text-amber-700 border-amber-200' :
                                  fullAccount.icpFit === 'Disqualified' ? 'bg-gray-100 text-gray-600 border-gray-300' :
                                  'bg-red-50 text-red-700 border-red-200'
                                }`}>{fullAccount.icpFit}</span>}
                              </div>
                              {fullAccount.scoreSummary && <div className="text-[11px] text-gray-500 leading-relaxed">{fullAccount.scoreSummary}</div>}
                            </div>

                            {/* Screening Warnings */}
                            {fullAccount.screeningWarnings && fullAccount.screeningWarnings.length > 0 && (
                              <div className="px-5 py-3 border-b border-amber-100 bg-amber-50/30">
                                <details>
                                  <summary className="flex items-center gap-2 cursor-pointer select-none">
                                    <span className="text-[10px] text-amber-600 uppercase tracking-wide font-semibold">⚠ Screening Warnings ({fullAccount.screeningWarnings.length})</span>
                                  </summary>
                                  <div className="mt-2 space-y-1">
                                    {fullAccount.screeningWarnings.map((w, i) => (
                                      <div key={i} className="text-[11px] text-amber-700 flex items-start gap-1.5">
                                        <span className="text-amber-400 mt-0.5">•</span>
                                        <span>{w}</span>
                                      </div>
                                    ))}
                                  </div>
                                </details>
                              </div>
                            )}

                            {/* Top Gaps — Opportunity framing */}
                            {(fullAccount.gap1Opportunity || fullAccount.gap2Opportunity) && (
                              <div className="px-5 py-3 border-b border-gray-100 space-y-2">
                                {fullAccount.gap1Opportunity && (
                                  <div className="p-2.5 bg-violet-50/50 rounded-lg border border-violet-200/50">
                                    <div className="flex items-center gap-2 mb-1">
                                      <span className="text-[9px] bg-violet-600 text-white px-1 py-0.5 rounded font-bold">①</span>
                                      <span className="text-[11px] text-violet-700 font-semibold">{fullAccount.gap1Factor}. {fullAccount.gap1Name}</span>
                                      <span className="text-[10px] text-violet-500">+{fullAccount.gap1Score}</span>
                                    </div>
                                    <div className="text-[11px] text-gray-600 leading-relaxed">{fullAccount.gap1Opportunity}</div>
                                  </div>
                                )}
                                {fullAccount.gap2Opportunity && (
                                  <div className="p-2.5 bg-violet-50/30 rounded-lg border border-violet-200/30">
                                    <div className="flex items-center gap-2 mb-1">
                                      <span className="text-[9px] bg-violet-400 text-white px-1 py-0.5 rounded font-bold">②</span>
                                      <span className="text-[11px] text-violet-600 font-semibold">{fullAccount.gap2Factor}. {fullAccount.gap2Name}</span>
                                      <span className="text-[10px] text-violet-400">+{fullAccount.gap2Score}</span>
                                    </div>
                                    <div className="text-[11px] text-gray-600 leading-relaxed">{fullAccount.gap2Opportunity}</div>
                                  </div>
                                )}
                              </div>
                            )}

                            {/* Scoring breakdown — gap #1 and #2 first */}
                            {fullAccount.scoringResult && (
                              <div className="px-5 py-4 border-b border-gray-100">
                                <div className="text-[9px] text-gray-500 uppercase tracking-wider font-semibold mb-3">Scoring Breakdown</div>
                                <div className="space-y-4">
                                  {(() => {
                                    const allFactors = [
                                      { key: 'A', label: 'A. Differentiation', score: fullAccount.scoreA, just: fullAccount.scoreAJust, color: 'text-purple-400', borderColor: 'border-purple-500/20' },
                                      { key: 'B', label: 'B. Outcomes', score: fullAccount.scoreB, just: fullAccount.scoreBJust, color: 'text-rose-600', borderColor: 'border-rose-500/20' },
                                      { key: 'C', label: 'C. Customer-centric', score: fullAccount.scoreC, just: fullAccount.scoreCJust, color: 'text-orange-400', borderColor: 'border-orange-500/20' },
                                      { key: 'D', label: 'D. Product change', score: fullAccount.scoreD, just: fullAccount.scoreDJust, color: 'text-cyan-400', borderColor: 'border-cyan-500/20' },
                                      { key: 'E', label: 'E. Audience change', score: fullAccount.scoreE, just: fullAccount.scoreEJust, color: 'text-sky-600', borderColor: 'border-sky-500/20' },
                                      { key: 'F', label: 'F. Multi-product', score: fullAccount.scoreF, just: fullAccount.scoreFJust, color: 'text-violet-600', borderColor: 'border-violet-500/20' },
                                      { key: 'G', label: 'G. Vision Gap', score: fullAccount.scoreG, just: fullAccount.scoreGJust, color: 'text-pink-600', borderColor: 'border-pink-500/20' },
                                    ];
                                    const g1 = fullAccount.gap1Factor ? allFactors.find(f => f.key === fullAccount.gap1Factor) : null;
                                    const g2 = fullAccount.gap2Factor ? allFactors.find(f => f.key === fullAccount.gap2Factor) : null;
                                    const topKeys = new Set([fullAccount.gap1Factor, fullAccount.gap2Factor].filter(Boolean));
                                    const rest = allFactors.filter(f => !topKeys.has(f.key));
                                    const ordered = [...(g1 ? [g1] : []), ...(g2 ? [g2] : []), ...rest];
                                    return ordered.map(({ key, label, score, just, color, borderColor }) => {
                                      const isTop = topKeys.has(key);
                                      const gapNum = key === fullAccount.gap1Factor ? '①' : key === fullAccount.gap2Factor ? '②' : null;
                                      return (
                                        <div key={key}>
                                          <div className="flex items-center gap-2 mb-1.5">
                                            {gapNum && <span className="text-[10px] bg-violet-600 text-white px-1 py-0.5 rounded font-bold leading-none">{gapNum}</span>}
                                            <span className={`${color} font-semibold text-xs`}>{label}</span>
                                            <span className="flex gap-0.5">
                                              {[1,2,3].map(n => (
                                                <span key={n} className={`inline-block w-3.5 h-1.5 rounded-full ${n <= score ? (score === 3 ? 'bg-green-400' : score === 2 ? 'bg-amber-400' : 'bg-red-400') : 'bg-gray-200'}`} />
                                              ))}
                                            </span>
                                            <span className="text-gray-500 text-[10px]">+{score}</span>
                                          </div>
                                          <div className={`p-2.5 bg-gray-50/60 rounded-md border ${isTop ? 'border-violet-300/40' : borderColor}`}>
                                            <FactorPanel factorKey={key} data={just} />
                                          </div>
                                        </div>
                                      );
                                    });
                                  })()}
                                </div>
                              </div>
                            )}

                            {/* Contacts at this account */}
                            <div className="px-5 py-4 border-b border-gray-100">
                              <div className="flex items-center justify-between mb-3">
                                <div className="text-xs text-gray-500 uppercase tracking-wide font-medium">Contacts ({accountContacts.length})</div>
                                {accountContacts.filter(ac => !ac.business_email && ac.linkedin).length > 0 && (
                                  <button onClick={() => enrichContactsBulk(accountContacts)}
                                    className="px-2.5 py-1 bg-violet-50 hover:bg-violet-100 text-violet-600 rounded-lg text-[10px] font-medium border border-violet-200">
                                    Enrich Emails ({accountContacts.filter(ac => !ac.business_email && ac.linkedin).length})
                                  </button>
                                )}
                              </div>
                              {accountContacts.length > 0 ? (
                                <div className="space-y-1.5">
                                  {accountContacts.slice(0, 20).map(ac => (
                                    <div key={ac.id} className={`flex items-center gap-3 px-3 py-2 rounded cursor-pointer ${ac.id === ct.id ? 'bg-violet-50 border border-violet-200' : 'bg-gray-50 hover:bg-gray-100'}`}
                                      onClick={() => setSelectedCampaignContact(ac.id)}>
                                      <div className="flex-1 min-w-0">
                                        <div className="text-[11px] text-gray-900 font-medium truncate">{ac.name}</div>
                                        <div className="text-[10px] text-gray-500 truncate">{ac.title}</div>
                                      </div>
                                      <div className="text-[10px] text-gray-400">{ac.seniority}</div>
                                      {ac.business_email && <span className="text-[9px] text-emerald-600 truncate max-w-[120px]" title={ac.business_email}>✉</span>}
                                    </div>
                                  ))}
                                </div>
                              ) : (
                                <div className="text-[11px] text-gray-400">No contacts yet.</div>
                              )}
                            </div>

                            {/* Research report */}
                            <div className="px-5 py-3 border-b border-gray-100">
                              <ResearchReport company={fullAccount} />
                            </div>
                          </>
                        ) : companyBasic ? (
                          <div className="px-5 py-6 text-center text-gray-400 text-[11px]">Account "{companyBasic.name}" has not been screened yet.</div>
                        ) : (
                          <div className="px-5 py-6 text-center text-gray-400 text-[11px]">No linked account.</div>
                        )}
                      </div>
                    );
                  })() : (
                    <div className="flex flex-col items-center justify-center h-full text-gray-400">
                      <div className="text-4xl mb-3">👤</div>
                      <div className="text-sm">Select a contact</div>
                    </div>
                  )}
                </div>

                {/* DRAG HANDLE 2: between middle and right */}
                <div className="w-1 cursor-col-resize bg-gray-200 hover:bg-violet-300 active:bg-violet-400 transition-colors flex-shrink-0"
                  onMouseDown={(e) => {
                    e.preventDefault();
                    const startX = e.clientX;
                    const startW = campPanelWidths[1];
                    const onMove = (ev) => {
                      const delta = ev.clientX - startX;
                      setCampPanelWidths(prev => [prev[0], Math.max(200, Math.min(600, startW + delta)), prev[2]]);
                    };
                    const onUp = () => { document.removeEventListener('mousemove', onMove); document.removeEventListener('mouseup', onUp); };
                    document.addEventListener('mousemove', onMove);
                    document.addEventListener('mouseup', onUp);
                  }} />

                {/* RIGHT: Email Template Builder + Generated Messages */}
                <div className="flex-1 flex flex-col min-w-[300px] overflow-hidden">
                  {/* Action bar */}
                  <div className="flex items-center justify-between px-5 py-2.5 border-b border-gray-100">
                    <div className="flex items-center gap-2">
                      <span className="text-[11px] text-gray-400 uppercase tracking-wide font-medium">Email Builder</span>
                      {generating && genProgress && <span className="text-[11px] text-violet-600 animate-pulse">{genProgress}</span>}
                    </div>
                    <div className="relative group">
                      <button disabled={generating || campaignContacts.length === 0}
                        className="px-3 py-1 bg-gray-100 hover:bg-gray-200 disabled:bg-gray-50 disabled:text-gray-300 text-gray-600 rounded-lg text-[11px] font-medium border border-gray-200">
                        Generate All ▾
                      </button>
                      <div className="absolute right-0 top-7 z-50 bg-white border border-gray-200 rounded-lg shadow-xl py-1 min-w-[160px] hidden group-hover:block">
                        {[3, 5, 10].map(n => (
                          <button key={n} onClick={() => handleTestOnMultiple(n)} disabled={generating}
                            className="block w-full text-left px-4 py-1.5 text-[12px] text-gray-600 hover:bg-violet-50 hover:text-violet-600 disabled:opacity-50">
                            {n} contacts
                          </button>
                        ))}
                        <button onClick={() => handleTestOnMultiple('all')} disabled={generating}
                          className="block w-full text-left px-4 py-1.5 text-[12px] text-violet-600 font-medium hover:bg-violet-50 disabled:opacity-50 border-t border-gray-100">
                          All {campaignContacts.length} contacts
                        </button>
                      </div>
                    </div>
                  </div>

                  <div className="flex-1 overflow-auto">
                    {/* Compute selected contact's account for factor defaults */}
                    {(() => {
                      const selCc = campaignContacts.find(c => c.contact_id === selectedCampaignContact);
                      const selAccount = selCc?.contacts?.companies?.domain ? companies.find(c => c.domain === selCc.contacts.companies.domain && c.status === 'complete') : null;
                      // Compute gap factors from scores if not stored
                      let defaultGap1 = selAccount?.gap1Factor || '';
                      let defaultGap2 = selAccount?.gap2Factor || '';
                      if (selAccount && (!defaultGap1 || !defaultGap2)) {
                        const scores = { A: selAccount.scoreA || 0, B: selAccount.scoreB || 0, C: selAccount.scoreC || 0, D: selAccount.scoreD || 0, E: selAccount.scoreE || 0, F: selAccount.scoreF || 0, G: selAccount.scoreG || 0 };
                        const sorted = Object.entries(scores).sort((a, b) => b[1] - a[1] || GAP_TIEBREAKER.indexOf(a[0]) - GAP_TIEBREAKER.indexOf(b[0]));
                        if (!defaultGap1) defaultGap1 = sorted[0]?.[0] || 'A';
                        if (!defaultGap2) defaultGap2 = sorted[1]?.[0] || 'B';
                      }
                      if (!defaultGap1) defaultGap1 = 'A';
                      if (!defaultGap2) defaultGap2 = 'B';
                      const activeGap1 = email1FactorOverride || defaultGap1;
                      const activeGap2 = email2FactorOverride || defaultGap2;
                      return (<>
                    {/* Generated count */}
                    {genContactsWithMessages.size > 0 && (
                      <div className="px-5 py-2 border-b border-gray-100">
                        <div className="flex items-center gap-2">
                          <div className="flex-1 h-1.5 bg-gray-100 rounded-full overflow-hidden">
                            <div className="h-full bg-green-400 rounded-full transition-all" style={{ width: `${(genContactsWithMessages.size / campaignContacts.length) * 100}%` }} />
                          </div>
                          <span className="text-[10px] text-gray-400">{genContactsWithMessages.size}/{campaignContacts.length} generated</span>
                        </div>
                      </div>
                    )}

                    {/* EMAIL 1 TEMPLATE */}
                    <div className="px-5 py-4 border-b border-gray-100">
                      <div className="flex items-center justify-between mb-3">
                        <div className="flex items-center gap-2">
                          <span className="text-[10px] bg-violet-600 text-white px-1.5 py-0.5 rounded font-bold">Email 1</span>
                          <span className="text-xs text-gray-600 font-medium">Primary gap email</span>
                        </div>
                        <div className="flex items-center gap-2">
                          <select value={activeGap1}
                            onChange={e => setEmail1FactorOverride(e.target.value)}
                            className="bg-white border border-gray-200 rounded px-2 py-1 text-[11px] text-gray-600">
                            {['A','B','C','D','E','F','G'].map(f => (
                              <option key={f} value={f}>{f}. {FACTOR_NAMES[f]}{f === defaultGap1 ? ' ★' : ''}</option>
                            ))}
                          </select>
                          <button onClick={() => handleGenerateSingleEmail(1)}
                            disabled={generating || !selectedCampaignContact}
                            className="px-3 py-1 bg-violet-600 hover:bg-violet-500 disabled:bg-violet-600/40 disabled:cursor-not-allowed text-white rounded-lg text-[11px] font-medium">
                            {generating && genProgress.includes('Email 1') ? 'Generating...' : 'Generate Email 1'}
                          </button>
                        </div>
                      </div>

                      {/* Sonnet-generated section (opening) */}
                      <div className="mb-3">
                        <div className="text-[10px] text-gray-500 uppercase tracking-wide font-medium mb-1">Opening Paragraph <span className="text-violet-500 normal-case">(AI-generated)</span></div>
                        <div className="p-3 bg-violet-50/30 border border-violet-200/30 rounded-lg text-xs text-gray-600 italic leading-relaxed">
                          Sonnet writes a 3-4 sentence opening paragraph that taps into the founder's anxiety about being lumped in with competitors, states what makes them different, and frames a homepage/GTM opportunity. This is the only AI-generated part of the email.
                        </div>
                      </div>

                      {/* Code-assembled template */}
                      <div>
                        <div className="text-[10px] text-gray-500 uppercase tracking-wide font-medium mb-1">Template <span className="text-gray-400 normal-case">(code-assembled)</span></div>
                        <div className="p-3 bg-gray-50 border border-gray-200 rounded-lg text-xs text-gray-600 font-mono leading-relaxed whitespace-pre-wrap">
                          <span className="text-violet-600">{'{{firstName}}'}</span>,{'\n\n'}
                          <span className="bg-violet-100/50 text-violet-700 px-1 rounded">{'{{opening}}'}</span>{'\n\n'}
                          <span className="text-gray-500">{['B','C'].includes(activeGap1) ? emailTemplates.credibility_bc : emailTemplates.credibility_adg}</span>{'\n\n'}
                          As a starting point, I offer a free story gap assessment that could surface a few ways your story could better land with <span className="text-violet-600">{'{{buyerTitle}}'}</span>.{'\n\n'}
                          Mike
                        </div>
                      </div>
                    </div>

                    {/* EMAIL 2 TEMPLATE */}
                    <div className="px-5 py-4 border-b border-gray-100">
                      <div className="flex items-center justify-between mb-3">
                        <div className="flex items-center gap-2">
                          <span className="text-[10px] bg-violet-400 text-white px-1.5 py-0.5 rounded font-bold">Email 2</span>
                          <span className="text-xs text-gray-600 font-medium">Follow-up email</span>
                        </div>
                        <div className="flex items-center gap-2">
                          <select value={activeGap2}
                            onChange={e => setEmail2FactorOverride(e.target.value)}
                            className="bg-white border border-gray-200 rounded px-2 py-1 text-[11px] text-gray-600">
                            {['A','B','C','D','E','F','G'].map(f => (
                              <option key={f} value={f}>{f}. {FACTOR_NAMES[f]}{f === defaultGap2 ? ' ★' : ''}</option>
                            ))}
                          </select>
                          <button onClick={() => handleGenerateSingleEmail(2)}
                            disabled={generating || !selectedCampaignContact}
                            className="px-3 py-1 bg-violet-400 hover:bg-violet-300 disabled:bg-violet-400/40 disabled:cursor-not-allowed text-white rounded-lg text-[11px] font-medium">
                            {generating && genProgress.includes('Email 2') ? 'Generating...' : 'Generate Email 2'}
                          </button>
                        </div>
                      </div>

                      <div className="mb-3">
                        <div className="text-[10px] text-gray-500 uppercase tracking-wide font-medium mb-1">Observation <span className="text-violet-500 normal-case">(AI-generated)</span></div>
                        <div className="p-3 bg-violet-50/30 border border-violet-200/30 rounded-lg text-xs text-gray-600 italic leading-relaxed">
                          Sonnet writes one short follow-up observation referencing the second gap. Starts with "One other thing I noticed" or similar.
                        </div>
                      </div>

                      <div>
                        <div className="text-[10px] text-gray-500 uppercase tracking-wide font-medium mb-1">Template <span className="text-gray-400 normal-case">(code-assembled)</span></div>
                        <div className="p-3 bg-gray-50 border border-gray-200 rounded-lg text-xs text-gray-600 font-mono leading-relaxed whitespace-pre-wrap">
                          <span className="text-violet-600">{'{{firstName}}'}</span>,{'\n\n'}
                          <span className="bg-violet-100/50 text-violet-700 px-1 rounded">{'{{opening}}'}</span>{'\n\n'}
                          {emailTemplates.email2_closing}{'\n\n'}
                          Mike
                        </div>
                      </div>
                    </div>

                    {/* GAP SELECTION */}
                    {contactGapSelection && (
                      <div className="px-5 py-3 border-b border-gray-100">
                        <div className="bg-violet-50/50 border border-violet-200/50 rounded-lg px-4 py-3">
                          <div className="text-[10px] text-violet-600 uppercase tracking-wide font-semibold mb-2">Gap Selection</div>
                          <div className="flex gap-2 flex-wrap mb-2">
                            {contactGapSelection.scores && Object.entries(contactGapSelection.scores).map(([letter, score]) => (
                              <div key={letter} className={`text-[11px] px-2 py-0.5 rounded ${
                                letter === contactGapSelection.email1_gap ? 'bg-violet-200 text-violet-800 font-bold' :
                                letter === contactGapSelection.email2_gap ? 'bg-violet-100 text-violet-700 font-medium' :
                                'bg-gray-100 text-gray-500'
                              }`}>{letter}: {score}</div>
                            ))}
                          </div>
                          <div className="text-[11px] text-violet-800">
                            Email 1: <span className="font-medium">{contactGapSelection.email1_gap}. {contactGapSelection.email1_gap_name}</span> (+{contactGapSelection.email1_score})
                            {' · '}
                            Email 2: <span className="font-medium">{contactGapSelection.email2_gap}. {contactGapSelection.email2_gap_name}</span> (+{contactGapSelection.email2_score})
                          </div>
                        </div>
                      </div>
                    )}

                    {/* PROMPT INPUTS (collapsible, shows what was passed to Sonnet) */}
                    {(lastPromptInputs.email1 || lastPromptInputs.email2) && (
                      <div className="px-5 py-3 border-b border-gray-100">
                        {[lastPromptInputs.email1, lastPromptInputs.email2].filter(Boolean).map((inputs, idx) => (
                          <details key={idx} className="mb-2 last:mb-0">
                            <summary className="flex items-center gap-2 cursor-pointer select-none py-1 text-[10px]">
                              <span className="text-gray-500 uppercase tracking-wide font-semibold">Email {idx + 1} Inputs</span>
                              <span className="text-violet-500">{inputs.factor}. {inputs.factorName}</span>
                              <span className="text-gray-300">▶</span>
                            </summary>
                            <div className="mt-2 p-3 bg-gray-50 rounded-lg border border-gray-200 space-y-2 text-[11px]">
                              <div><span className="text-gray-500 font-medium">Company:</span> <span className="text-gray-700">{inputs.companyName}</span></div>
                              <div><span className="text-gray-500 font-medium">Factor:</span> <span className="text-gray-700">{inputs.factor}. {inputs.factorName}</span></div>
                              <div>
                                <span className="text-gray-500 font-medium">Target buyer:</span>{' '}
                                <span className={inputs.targetBuyer ? 'text-gray-600' : 'text-red-400 italic'}>{inputs.targetBuyer || '⚠ EMPTY — re-screen this company'}</span>
                              </div>
                              <div>
                                <span className="text-gray-500 font-medium">Competitors:</span>{' '}
                                {inputs.competitors ? (
                                  <span className="text-gray-600 whitespace-pre-wrap">{inputs.competitors.substring(0, 500)}</span>
                                ) : (
                                  <span className="text-red-400 italic">⚠ EMPTY — re-screen this company</span>
                                )}
                              </div>
                              <div>
                                <span className="text-gray-500 font-medium">Differentiators:</span>
                                {inputs.differentiators && inputs.differentiators.length > 0 ? (
                                  <div className="ml-2 mt-0.5 text-gray-600">{inputs.differentiators.map((d, i) => <div key={i}>{i+1}. {d}</div>)}</div>
                                ) : (
                                  <span className="text-red-400 italic"> ⚠ EMPTY — re-screen this company</span>
                                )}
                              </div>
                              <div><span className="text-gray-500 font-medium">Verdict:</span> <span className={inputs.verdict ? 'text-gray-600' : 'text-red-400 italic'}>{inputs.verdict || '⚠ EMPTY'}</span></div>
                              <div>
                                <span className="text-gray-500 font-medium">Homepage hero:</span>{' '}
                                <span className={inputs.heroText ? 'text-gray-600' : 'text-red-400 italic'}>{inputs.heroText ? inputs.heroText.substring(0, 300) + '...' : '⚠ EMPTY'}</span>
                              </div>
                              <div><span className="text-gray-500 font-medium">Training examples:</span> <span className="text-gray-600">{inputs.exampleCount !== undefined ? `${inputs.exampleCount} matched` : 'N/A'}</span></div>
                            </div>
                          </details>
                        ))}
                      </div>
                    )}

                    {/* GENERATED MESSAGES (editable) */}
                    {contactGenMessages.length > 0 && (
                      <div className="px-5 py-4">
                        <div className="flex items-center gap-2 mb-3">
                          <span className="text-[10px] text-green-600 uppercase tracking-wide font-semibold">Generated Output</span>
                          <span className="text-[10px] text-gray-400">({contactGenMessages.length} messages)</span>
                        </div>
                        {contactGenMessages.map(msg => (
                          <div key={msg.id} className="mb-4">
                            <GeneratedMessageCard msg={msg} onSave={handleSaveGeneratedMessage} />
                          </div>
                        ))}
                      </div>
                    )}

                    {/* Empty state */}
                    {contactGenMessages.length === 0 && selectedCampaignContact && !generating && (
                      <div className="px-5 py-8 text-center">
                        <div className="text-gray-400 text-[13px] mb-2">No messages generated yet</div>
                        <div className="text-gray-300 text-[11px]">Use "Generate Email 1" or "Generate Email 2" above to create emails for this contact.</div>
                      </div>
                    )}
                      </>);
                    })()}

                    {!selectedCampaignContact && (
                      <div className="px-5 py-8 text-center text-gray-400 text-[13px]">Select a contact to view email templates and generate messages.</div>
                    )}

                    {/* Training Examples */}
                    {selectedCampaignContact && (() => {
                      const cc = campaignContacts.find(c => c.contact_id === selectedCampaignContact);
                      const fa = cc?.contacts?.companies?.domain ? companies.find(c => c.domain === cc.contacts.companies.domain && c.status === 'complete') : null;
                      if (!fa) return null;
                      return (
                        <div className="px-5 py-4 border-t border-gray-100">
                          <div className="flex items-center justify-between mb-3">
                            <div className="text-[10px] text-gray-500 uppercase tracking-wide font-semibold">Training Examples</div>
                            <div className="flex items-center gap-2">
                              <div className="flex gap-1">
                                {['A','B','C','D','E','F','G'].map(f => {
                                  const count = trainingExamples.filter(e => e.gap_factor === f).length;
                                  return <span key={f} className={`text-[9px] px-1 py-0.5 rounded ${count >= 3 ? 'bg-green-100 text-green-600' : count >= 1 ? 'bg-amber-100 text-amber-600' : 'bg-gray-100 text-gray-400'}`}>{f}({count})</span>;
                                })}
                              </div>
                              <button onClick={() => {
                                setManualEmail({
                                  gap_factor: fa.gap1Factor || '',
                                  gap_name: fa.gap1Name || '',
                                  opening: '', context: '', avoid_notes: ''
                                });
                                setShowManualEmailEditor(true);
                              }} className="px-2.5 py-1 bg-violet-50 hover:bg-violet-100 text-violet-600 rounded text-[10px] font-medium border border-violet-200">
                                + Add Opening Example
                              </button>
                            </div>
                          </div>
                          
                          {showManualEmailEditor && (
                            <div className="p-3 bg-gray-50 rounded-lg border border-gray-200 mb-3 space-y-2.5">
                              <div>
                                <label className="block text-[10px] text-gray-500 mb-0.5">Gap Factor</label>
                                <select value={manualEmail.gap_factor} onChange={e => {
                                  const f = e.target.value;
                                  const names = { A: 'Differentiation', B: 'Outcomes', C: 'Customer-centric', D: 'Product Change', E: 'Audience Change', F: 'Multi-product', G: 'Vision Gap' };
                                  setManualEmail(prev => ({ ...prev, gap_factor: f, gap_name: names[f] || '' }));
                                }} className="w-full bg-white border border-gray-200 rounded px-2 py-1.5 text-xs text-gray-700">
                                  <option value="">Select gap...</option>
                                  {['A','B','C','D','E','F','G'].map(f => {
                                    const names = { A: 'Differentiation', B: 'Outcomes', C: 'Customer-centric', D: 'Product Change', E: 'Audience Change', F: 'Multi-product', G: 'Vision Gap' };
                                    const isTop = f === fa.gap1Factor || f === fa.gap2Factor;
                                    return <option key={f} value={f}>{f}. {names[f]}{isTop ? ' ★' : ''}</option>;
                                  })}
                                </select>
                              </div>
                              <div>
                                <label className="block text-[10px] text-gray-500 mb-0.5">Opening paragraph (the AI-generated part only)</label>
                                <textarea value={manualEmail.opening} onChange={e => setManualEmail(prev => ({ ...prev, opening: e.target.value }))}
                                  rows={5} className="w-full bg-white border border-gray-200 rounded px-2 py-1.5 text-xs text-gray-700 leading-relaxed" placeholder="Write the opening paragraph you wish the model would produce. Just the opening, not the full email." />
                              </div>
                              <div>
                                <label className="block text-[10px] text-gray-500 mb-0.5">Context (what was the gap about for this company?)</label>
                                <textarea value={manualEmail.context} onChange={e => setManualEmail(prev => ({ ...prev, context: e.target.value }))}
                                  rows={2} className="w-full bg-white border border-gray-200 rounded px-2 py-1.5 text-xs text-gray-700" placeholder="e.g. Strongest differentiator: agents that continuously adjust forecasts. Competitors: Kinaxis, o9." />
                              </div>
                              <div>
                                <label className="block text-[10px] text-gray-500 mb-0.5">Why it works (what makes this example good)</label>
                                <textarea value={manualEmail.avoid_notes} onChange={e => setManualEmail(prev => ({ ...prev, avoid_notes: e.target.value }))}
                                  rows={2} className="w-full bg-white border border-gray-200 rounded px-2 py-1.5 text-xs text-gray-700" placeholder="e.g. Opens with lumping-in fear. Single differentiator. Homepage frames opportunity." />
                              </div>
                              <div className="flex gap-2">
                                <button onClick={async () => {
                                  if (!manualEmail.gap_factor || !manualEmail.opening.trim()) return;
                                  try {
                                    const cc = campaignContacts.find(c => c.contact_id === selectedCampaignContact);
                                    const ct = cc?.contacts;
                                    const saved = await createTrainingExample({
                                      gap_factor: manualEmail.gap_factor, gap_name: manualEmail.gap_name,
                                      opening: manualEmail.opening, context: manualEmail.context, avoid_notes: manualEmail.avoid_notes,
                                      company_id: fa?.dbCompanyId || null, company_name: fa?.companyName || '',
                                      contact_name: ct?.name || '',
                                    });
                                    if (saved) { setTrainingExamples(prev => [saved, ...prev]); setShowManualEmailEditor(false); addLog(`Saved opening example for Factor ${manualEmail.gap_factor}`); }
                                  } catch (err) { addLog(`Error saving example: ${err.message}`); }
                                }} disabled={!manualEmail.gap_factor || !manualEmail.opening.trim()}
                                  className="px-3 py-1.5 bg-violet-600 hover:bg-violet-500 disabled:bg-violet-600/50 text-white rounded text-xs font-medium">
                                  Save Opening Example
                                </button>
                                <button onClick={() => setShowManualEmailEditor(false)} className="px-3 py-1.5 text-gray-400 hover:text-gray-600 text-xs">Cancel</button>
                              </div>
                            </div>
                          )}
                          
                          {trainingExamples.filter(e => e.gap_factor === fa.gap1Factor || e.gap_factor === fa.gap2Factor).length > 0 && (
                            <div className="space-y-2">
                              {trainingExamples.filter(e => e.gap_factor === fa.gap1Factor || e.gap_factor === fa.gap2Factor).map(ex => (
                                <div key={ex.id} className="p-2.5 bg-white rounded border border-gray-200 text-[11px]">
                                  <div className="flex items-center gap-2 mb-1">
                                    <span className="px-1.5 py-0.5 bg-violet-100 text-violet-700 rounded text-[9px] font-semibold">{ex.gap_factor}. {ex.gap_name}</span>
                                    <span className="text-gray-400 text-[9px]">Email {ex.email_number}</span>
                                    <span className="text-gray-300 text-[9px]">{ex.company_name}</span>
                                    <button onClick={async () => { await deleteTrainingExample(ex.id); setTrainingExamples(prev => prev.filter(e => e.id !== ex.id)); }}
                                      className="ml-auto text-gray-300 hover:text-red-500 text-[10px]">×</button>
                                  </div>
                                  {ex.context && <div className="text-gray-400 text-[10px] mb-0.5">{ex.context}</div>}
                                  <div className="text-gray-500 whitespace-pre-wrap leading-relaxed">{(ex.opening || ex.body || '').substring(0, 200)}{(ex.opening || ex.body || '').length > 200 ? '...' : ''}</div>
                                  {ex.avoid_notes && <div className="mt-1 text-[10px] text-amber-600">⚠ {ex.avoid_notes}</div>}
                                </div>
                              ))}
                            </div>
                          )}
                        </div>
                      );
                    })()}
                  </div>
                </div>
                </div>
              </div>
            )}

            {/* Add contacts to campaign modal */}
          </div>
        )}

        {debugLog.length > 0 && (
          <details className="mt-6">
            <summary className="text-xs text-gray-500 cursor-pointer hover:text-gray-400">Debug Log ({debugLog.length} entries)</summary>
            <div className="mt-2 p-3 bg-white border border-gray-200 rounded-lg max-h-48 overflow-auto">
              {debugLog.slice(-50).map((log, i) => <div key={i} className="text-xs text-gray-500 font-mono">{log}</div>)}
            </div>
          </details>
        )}
        {showCsvModal && (
          <div className="fixed inset-0 bg-black/40 flex items-center justify-center z-50 p-8">
            <div className="bg-white border border-gray-200 rounded-lg p-6 max-w-3xl w-full max-h-[80vh] flex flex-col">
              <div className="flex justify-between items-center mb-4">
                <h3 className="text-lg font-bold text-gray-900">CSV Output</h3>
                <button onClick={() => setShowCsvModal(false)} className="text-gray-500 hover:text-gray-900">Close</button>
              </div>
              <textarea value={csvContent} readOnly className="flex-1 bg-gray-100 border border-gray-200 rounded p-3 text-xs font-mono text-gray-400 resize-none" />
              <button onClick={() => navigator.clipboard.writeText(csvContent)} className="mt-3 px-4 py-2 bg-violet-600 hover:bg-violet-500 text-white rounded-lg text-sm font-medium self-end">Copy to Clipboard</button>
            </div>
          </div>
        )}

        {/* ======= CONTACTS MODAL ======= */}
        {activeView === 'discover_contacts' && (
          <div className="bg-white border border-gray-200 rounded-lg flex flex-col" style={{ height: '85vh' }}>
              {/* Header */}
              <div className="flex items-center justify-between px-6 py-4 border-b border-gray-200">
                <div>
                  <h3 className="text-lg font-bold text-gray-900">
                    {Array.isArray(contactsCompany)
                      ? `Contacts across ${contactsCompany.length} accounts`
                      : contactsCompany ? `Contacts at ${contactsCompany.name}` : 'Discover Contacts'}
                  </h3>
                  <p className="text-xs text-gray-500 mt-0.5 max-w-[600px] truncate">
                    {Array.isArray(contactsCompany)
                      ? contactsCompany.map(c => c.name).join(', ')
                      : contactsCompany ? contactsCompany.domain : 'Use "Find Contacts" on an account, or add a Company Domain filter below'}
                    {contactsTotal > 0 && ` \u00b7 ${contactsTotal.toLocaleString()} total matches`}
                  </p>
                </div>
              </div>
              
              {/* Filters */}
              <div className="px-6 py-4 border-b border-gray-100 space-y-3">
                {/* Dynamic filter rows */}
                <div className="space-y-2">
                  {peopleFilters.map(f => {
                    const spec = PEOPLE_FILTER_CATALOG.find(s => s.key === f.fieldKey);
                    if (!spec) return null;
                    const PEOPLE_OP_LABELS = { '=': 'equals', '!=': 'not equals', '>': '>', '<': '<', '=>': '≥', '=<': '≤', 'in': 'in', 'not_in': 'not in', '(.)': 'contains', '[.]': 'exact match' };
                    return (
                      <div key={f.id} className="flex items-center gap-2 bg-gray-50 rounded-lg px-3 py-2">
                        <span className="text-xs text-gray-600 font-medium min-w-[120px]">{spec.label}</span>
                        {spec.operators.length > 1 && (
                          <select value={f.operator} onChange={e => updatePeopleFilter(f.id, { operator: e.target.value })}
                            className="bg-white border border-gray-200 rounded px-1.5 py-1 text-[11px] text-gray-600">
                            {spec.operators.map(op => <option key={op} value={op}>{PEOPLE_OP_LABELS[op] || op}</option>)}
                          </select>
                        )}
                        {/* Value input based on type */}
                        {spec.inputType === 'boolean' ? (
                          <span className="text-xs text-green-600 font-medium">Yes</span>
                        ) : spec.inputType === 'multi_select' ? (
                          <div className="flex flex-wrap gap-1 flex-1">
                            {(spec.options || []).map(opt => (
                              <button key={opt} onClick={() => {
                                const arr = Array.isArray(f.value) ? f.value : [];
                                updatePeopleFilter(f.id, { value: arr.includes(opt) ? arr.filter(x => x !== opt) : [...arr, opt] });
                              }}
                                className={`px-2 py-0.5 rounded text-[10px] transition-colors ${
                                  (Array.isArray(f.value) ? f.value : []).includes(opt)
                                    ? 'bg-violet-100 text-violet-700 border border-violet-300'
                                    : 'bg-white text-gray-500 border border-gray-200 hover:text-gray-700'
                                }`}>{opt}</button>
                            ))}
                          </div>
                        ) : spec.inputType === 'number' ? (
                          <input type="number" value={f.value} onChange={e => updatePeopleFilter(f.id, { value: e.target.value })}
                            className="w-24 bg-white border border-gray-200 rounded px-2 py-1 text-xs text-gray-700" placeholder="Value" />
                        ) : spec.inputType === 'people_autocomplete_text' ? (
                          <div className="relative flex-1">
                            <input type="text" value={f.value}
                              onChange={e => { updatePeopleFilter(f.id, { value: e.target.value }); if (spec.autocompleteField) fetchPeopleAutocomplete(f.id, spec.autocompleteField, e.target.value); }}
                              onFocus={e => { if (e.target.value && spec.autocompleteField) fetchPeopleAutocomplete(f.id, spec.autocompleteField, e.target.value); }}
                              onBlur={() => setTimeout(() => setPeopleAutocompleteResults(prev => ({ ...prev, [f.id]: [] })), 200)}
                              className="w-full bg-white border border-gray-200 rounded px-2 py-1 text-xs text-gray-700" placeholder={`Search ${spec.label.toLowerCase()}...`} />
                            {(peopleAutocompleteResults[f.id] || []).length > 0 && (
                              <div className="absolute left-0 right-0 top-full mt-1 bg-white border border-gray-200 rounded-lg shadow-xl z-50 max-h-[180px] overflow-auto">
                                {peopleAutocompleteResults[f.id].map((s, i) => (
                                  <button key={i} onMouseDown={e => { e.preventDefault(); updatePeopleFilter(f.id, { value: typeof s === 'string' ? s : s.value || s }); setPeopleAutocompleteResults(prev => ({ ...prev, [f.id]: [] })); }}
                                    className="block w-full text-left px-3 py-1.5 text-xs text-gray-700 hover:bg-violet-50 hover:text-violet-700 truncate">
                                    {typeof s === 'string' ? s : s.value || s.label || JSON.stringify(s)}
                                  </button>
                                ))}
                              </div>
                            )}
                            {peopleAutocompleteLoading[f.id] && <div className="absolute right-2 top-1 text-[9px] text-gray-400">...</div>}
                          </div>
                        ) : (
                          <input type="text" value={f.value} onChange={e => updatePeopleFilter(f.id, { value: e.target.value })}
                            className="flex-1 bg-white border border-gray-200 rounded px-2 py-1 text-xs text-gray-700" placeholder="Value" />
                        )}
                        <button onClick={() => removePeopleFilter(f.id)} className="text-gray-400 hover:text-red-500 text-xs">×</button>
                      </div>
                    );
                  })}
                </div>

                {/* Add filter button + dropdown */}
                <div className="flex items-center gap-2">
                  <div className="relative" ref={peopleFilterPickerRef}>
                    <button onClick={() => setShowPeopleFilterPicker(!showPeopleFilterPicker)}
                      className="px-3 py-1.5 bg-gray-100 hover:bg-gray-200 text-gray-600 rounded-lg text-xs border border-gray-200">
                      + Add Filter
                    </button>
                    {showPeopleFilterPicker && (
                      <div className="absolute left-0 top-full mt-1 bg-white border border-gray-200 rounded-lg shadow-xl z-50 min-w-[220px] max-h-[300px] overflow-auto">
                        {[...new Set(PEOPLE_FILTER_CATALOG.map(f => f.category))].map(cat => (
                          <div key={cat}>
                            <div className="px-3 py-1 text-[9px] text-gray-400 uppercase tracking-wider font-semibold bg-gray-50 sticky top-0">{cat}</div>
                            {PEOPLE_FILTER_CATALOG.filter(f => f.category === cat).map(f => (
                              <button key={f.key} onClick={() => addPeopleFilter(f.key)}
                                className="block w-full text-left px-3 py-1.5 text-xs text-gray-600 hover:bg-violet-50 hover:text-violet-700">
                                {f.label}
                              </button>
                            ))}
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                  {peopleFilters.length > 0 && (
                    <button onClick={searchContactsDynamic} disabled={contactsLoading}
                      className="px-4 py-1.5 bg-violet-600 hover:bg-violet-500 disabled:bg-violet-600/50 text-white rounded-lg text-xs font-medium">
                      {contactsLoading ? 'Searching...' : 'Search Contacts'}
                    </button>
                  )}
                </div>

                {/* Action buttons */}
                <div className="flex items-center gap-3">
                  {selectedContactResults.size > 0 && (
                    <button onClick={addContactsAndEnrich} disabled={addingContacts}
                      className="px-4 py-2 bg-violet-600 hover:bg-violet-500 disabled:bg-violet-600/50 text-white rounded-lg text-sm font-medium">
                      {addingContacts ? 'Adding & Enriching...' : `Add ${selectedContactResults.size} Contacts`}
                    </button>
                  )}
                  {selectedContactResults.size > 0 && (
                    <button onClick={() => setSelectedContactResults(new Set())} className="px-3 py-2 text-gray-400 hover:text-gray-600 text-xs">Clear selection</button>
                  )}
                  {contactsError && <span className="text-sm text-red-600">{contactsError}</span>}
                </div>
              </div>
              
              {/* Results */}
              <div className="flex-1 overflow-auto px-6 py-3">
                {contactsResults.length > 0 ? (
                  <div className="space-y-1">
                    {/* Column picker */}
                    <div className="flex items-center justify-end mb-2">
                      <div className="relative" ref={contactColumnPickerRef}>
                        <button onClick={() => setShowContactColumnPicker(!showContactColumnPicker)}
                          className="px-2.5 py-1 bg-gray-100 hover:bg-gray-200 text-gray-500 rounded text-[10px] border border-gray-200">
                          Columns ({contactResultColumns.length})
                        </button>
                        {showContactColumnPicker && (
                          <div className="absolute right-0 top-full mt-1 bg-white border border-gray-200 rounded-lg shadow-xl z-50 min-w-[180px] max-h-[280px] overflow-auto p-2">
                            {ALL_CONTACT_COLUMNS.map(col => (
                              <label key={col.key} className="flex items-center gap-2 px-2 py-1 hover:bg-gray-50 rounded cursor-pointer">
                                <input type="checkbox" checked={contactResultColumns.includes(col.key)}
                                  onChange={() => setContactResultColumns(prev => prev.includes(col.key) ? prev.filter(k => k !== col.key) : [...prev, col.key])}
                                  className="accent-violet-500 w-3 h-3" />
                                <span className="text-xs text-gray-600">{col.label}</span>
                              </label>
                            ))}
                            <button onClick={() => setContactResultColumns(DEFAULT_CONTACT_COLUMNS)}
                              className="w-full mt-1 px-2 py-1 text-[10px] text-gray-400 hover:text-gray-600 text-center border-t border-gray-100">Reset</button>
                          </div>
                        )}
                      </div>
                    </div>
                    {/* Table */}
                    <div className="overflow-x-auto">
                      <table className="w-full text-xs">
                        <thead>
                          <tr className="text-[10px] text-gray-400 uppercase tracking-wide font-medium sticky top-0 bg-white/95 backdrop-blur z-10">
                            <th className="text-left px-2 py-1.5 w-8">
                              <input type="checkbox"
                                checked={selectedContactResults.size === contactsResults.length && contactsResults.length > 0}
                                onChange={e => {
                                  if (e.target.checked) {
                                    setSelectedContactResults(new Set(contactsResults.map(p => p.id || p.linkedin)));
                                  } else {
                                    setSelectedContactResults(new Set());
                                  }
                                }}
                                className="rounded bg-gray-100 border-gray-600 text-violet-500 w-3.5 h-3.5" />
                            </th>
                            {contactResultColumns.map(k => {
                              const col = ALL_CONTACT_COLUMNS.find(c => c.key === k);
                              if (!col) return null;
                              return <th key={k} className={`text-left px-2 py-1.5 whitespace-nowrap ${col.type === 'number' ? 'text-right' : ''}`}>{col.label}</th>;
                            })}
                            <th className="text-right px-2 py-1.5 w-8">Links</th>
                          </tr>
                        </thead>
                        <tbody>
                          {contactsResults.map(p => {
                            const rowKey = p.id || p.linkedin;
                            const isChecked = selectedContactResults.has(rowKey);
                            return (
                              <tr key={rowKey} onClick={() => setSelectedContactResults(prev => {
                                const next = new Set(prev);
                                if (next.has(rowKey)) next.delete(rowKey); else next.add(rowKey);
                                return next;
                              })} className={`cursor-pointer transition-colors ${isChecked ? 'bg-violet-50' : 'hover:bg-gray-50'}`}>
                                <td className="px-2 py-2 w-8">
                                  <input type="checkbox" checked={isChecked} readOnly className="rounded bg-gray-100 border-gray-600 text-violet-500 w-3.5 h-3.5 pointer-events-none" />
                                </td>
                                {contactResultColumns.map(k => {
                                  const col = ALL_CONTACT_COLUMNS.find(c => c.key === k);
                                  if (!col) return null;
                                  const v = p[k];
                                  let display;
                                  if (k === 'name') {
                                    display = (
                                      <div>
                                        <span className="text-gray-900 font-medium">{v}</span>
                                        {p.emailVerified && <span className="text-[10px] text-emerald-600 ml-1">✓ email</span>}
                                        {p.recentJobChange && <span className="text-[10px] text-amber-600 ml-1">★ new</span>}
                                      </div>
                                    );
                                  } else if (col.type === 'date') {
                                    display = v ? new Date(v).toLocaleDateString('en-US', { year: 'numeric', month: 'short' }) : '-';
                                  } else if (col.type === 'number') {
                                    display = v ? v.toLocaleString() : '-';
                                  } else {
                                    display = v || '-';
                                  }
                                  return (
                                    <td key={k} className={`px-2 py-2 text-gray-500 truncate max-w-[200px] ${col.type === 'number' ? 'text-right' : ''}`} title={typeof v === 'string' ? v : ''}>
                                      {display}
                                    </td>
                                  );
                                })}
                                <td className="px-2 py-2 text-right w-8">
                                  {p.linkedin && (
                                    <a href={p.linkedin} target="_blank" rel="noopener noreferrer" onClick={e => e.stopPropagation()} className="text-violet-600 hover:text-violet-700 text-xs">LI</a>
                                  )}
                                </td>
                              </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    </div>
                  </div>
                ) : !contactsLoading ? (
                  <div className="text-center py-12 text-gray-500 text-sm">
                    Set filters and click Search to find contacts via Crustdata.
                  </div>
                ) : null}
                {contactsLoading && (
                  <div className="text-center py-8 text-sky-600 animate-pulse text-sm">Searching Crustdata...</div>
                )}
                {contactsCursor && !contactsLoading && (
                  <div className="text-center py-3">
                    <button onClick={() => searchContacts(true)}
                      className="px-5 py-2 bg-gray-100 hover:bg-gray-200 border border-gray-200 rounded text-sm text-gray-400">
                      Load More
                    </button>
                  </div>
                )}
              </div>
          </div>
        )}

        {/* ======= EMAIL PROMPTS VIEW (Database section) ======= */}
        {activeView === 'email_prompts' && (
          <div className="space-y-6">
            {/* Observation Prompts */}
            <div className="bg-white border border-gray-200 rounded-lg p-6">
              <h2 className="text-lg font-bold text-gray-900 mb-1">Observation Prompts</h2>
              <p className="text-sm text-gray-500 mb-4">Each gap type has its own prompt telling Sonnet how to write the email opening. These prompts define the angle, the contrast to look for, and the framing strategy for that gap.</p>
              <div className="space-y-4">
                {['A','B','C','D','E','F','G'].map(key => {
                  const labels = { A: 'Factor A — Differentiation', B: 'Factor B — Outcomes', C: 'Factor C — Customer-Centric', D: 'Factor D — Product Change', E: 'Factor E — Audience Change', F: 'Factor F — Multi-Product', G: 'Factor G — Vision Gap' };
                  const descs = {
                    A: 'How to frame an email when the company\'s real differentiators are buried or generic on the homepage',
                    B: 'How to frame an email when strategic outcomes are missing and the homepage leads with features',
                    C: 'How to frame an email when the homepage is product-centric instead of buyer-centric',
                    D: 'How to frame an email when the product has evolved but the homepage tells the old story',
                    E: 'How to frame an email when the company is targeting new buyers but the narrative speaks to the old audience',
                    F: 'How to frame an email when multiple products create a fragmented narrative',
                    G: 'How to frame an email when the CEO\'s public narrative differs from the homepage',
                  };
                  return (
                    <div key={key} className="border border-gray-200 rounded-lg overflow-hidden">
                      <div className="flex items-center justify-between px-4 py-2.5 bg-gray-50 border-b border-gray-200">
                        <div>
                          <div className="text-sm font-semibold text-gray-800">{labels[key]}</div>
                          <div className="text-[10px] text-gray-400">{descs[key]}</div>
                        </div>
                        <div className="flex items-center gap-2">
                          {obsPrompts[key] !== DEFAULT_OBS_PROMPTS[key] && (
                            <span className="text-[9px] text-amber-600 bg-amber-50 px-1.5 py-0.5 rounded">Modified</span>
                          )}
                          <button onClick={() => resetObsPrompt(key)} className="text-[10px] text-gray-400 hover:text-red-500">Reset</button>
                        </div>
                      </div>
                      <textarea value={obsPrompts[key]} onChange={e => updateObsPrompt(key, e.target.value)}
                        rows={8} className="w-full px-4 py-3 text-xs text-gray-700 font-mono leading-relaxed border-0 focus:outline-none focus:ring-1 focus:ring-violet-300 resize-y" />
                    </div>
                  );
                })}
              </div>
            </div>

            {/* Email Assembly Templates */}
            <div className="bg-white border border-gray-200 rounded-lg p-6">
              <h2 className="text-lg font-bold text-gray-900 mb-1">Email Assembly Templates</h2>
              <p className="text-sm text-gray-500 mb-4">These are the fixed parts of each email that get assembled by code around the AI-generated observation. Credibility, CTA, closing, and sign-off are never AI-generated.</p>
              <div className="space-y-4">
                {[
                  { key: 'credibility_adg', label: 'Credibility Block (Gaps A, D, E, F, G)', desc: 'Used when the top gap is Differentiation, Product Change, Audience Change, Multi-product, or Vision Gap', rows: 3 },
                  { key: 'credibility_bc', label: 'Credibility Block (Gaps B, C)', desc: 'Used when the top gap is Outcomes or Customer-Centric', rows: 3 },
                  { key: 'cta', label: 'CTA (Email 1)', desc: 'Call to action. Use {{buyerTitle}} for the target buyer title.', rows: 2 },
                  { key: 'email2_closing', label: 'Closing (Email 2)', desc: 'The fixed closing line for the follow-up email', rows: 2 },
                  { key: 'signoff', label: 'Sign-off', desc: 'Name at the bottom of every email', rows: 1 },
                ].map(({ key, label, desc, rows }) => (
                  <div key={key} className="border border-gray-200 rounded-lg overflow-hidden">
                    <div className="flex items-center justify-between px-4 py-2.5 bg-gray-50 border-b border-gray-200">
                      <div>
                        <div className="text-sm font-semibold text-gray-800">{label}</div>
                        <div className="text-[10px] text-gray-400">{desc}</div>
                      </div>
                      <div className="flex items-center gap-2">
                        {emailTemplates[key] !== DEFAULT_EMAIL_TEMPLATES[key] && (
                          <span className="text-[9px] text-amber-600 bg-amber-50 px-1.5 py-0.5 rounded">Modified</span>
                        )}
                        <button onClick={() => resetEmailTemplate(key)} className="text-[10px] text-gray-400 hover:text-red-500">Reset</button>
                      </div>
                    </div>
                    <textarea value={emailTemplates[key]} onChange={e => updateEmailTemplate(key, e.target.value)}
                      rows={rows} className="w-full px-4 py-3 text-xs text-gray-700 font-mono leading-relaxed border-0 focus:outline-none focus:ring-1 focus:ring-violet-300 resize-y" />
                  </div>
                ))}
              </div>
            </div>

            {/* Shared Rules (read-only reference) */}
            <div className="bg-white border border-gray-200 rounded-lg p-6">
              <h2 className="text-lg font-bold text-gray-900 mb-1">Shared Rules</h2>
              <p className="text-sm text-gray-500 mb-3">These rules are appended to every observation prompt automatically. They control tone, banned words, and formatting.</p>
              <div className="p-3 bg-gray-50 border border-gray-200 rounded-lg text-xs text-gray-600 font-mono leading-relaxed whitespace-pre-wrap">{OBS_SHARED_RULES}</div>
            </div>
          </div>
        )}

        {/* ======= DISCOVER RESEARCH PROMPT VIEW ======= */}
        {activeView === 'discover_prompts' && (
          <div className="bg-white border border-gray-200 rounded-lg p-6">
            <h2 className="text-lg font-bold text-gray-900 mb-1">Research Prompt</h2>
            <p className="text-sm text-gray-500 mb-4">This prompt is sent to Haiku to synthesize raw Exa data into a structured research report. It defines the output format that the scoring prompts read from.</p>
            <div className="border border-gray-200 rounded-lg overflow-hidden">
              <div className="flex items-center justify-between px-4 py-2.5 bg-gray-50 border-b border-gray-200">
                <div>
                  <div className="text-sm font-semibold text-gray-800">Research Synthesis Prompt</div>
                  <div className="text-[10px] text-gray-400">Defines all output fields: Product Summary, Target Customer, Outcomes, Differentiators, Homepage Sections, CEO Voice, etc.</div>
                </div>
                <div className="flex items-center gap-2">
                  {researchPrompt !== RESEARCH_PROMPT && (
                    <span className="text-[9px] text-amber-600 bg-amber-50 px-1.5 py-0.5 rounded">Modified</span>
                  )}
                  <button onClick={resetResearchPrompt}
                    className="text-[10px] text-gray-400 hover:text-red-500">Reset to Default</button>
                </div>
              </div>
              <textarea value={researchPrompt} onChange={e => updateResearchPrompt(e.target.value)}
                rows={30}
                className="w-full px-4 py-3 text-xs text-gray-700 font-mono leading-relaxed border-0 focus:outline-none focus:ring-1 focus:ring-violet-300 resize-y" />
            </div>
          </div>
        )}

        {/* ======= SCREENED ACCOUNTS VIEW ======= */}
        {activeView === 'screened' && (
          <div className="flex gap-4" style={{ height: '85vh' }}>
            {/* Left: list */}
            <div className="w-[420px] bg-white border border-gray-200 rounded-lg flex flex-col overflow-hidden flex-shrink-0">
              <div className="px-4 py-3 border-b border-gray-100">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-sm font-semibold text-gray-900">Screened Accounts ({stagedCompanies.length})</span>
                  <div className="flex gap-1.5">
                    {checkedStaged.size > 0 && (
                      <>
                        <button onClick={() => { bulkAddToDatabase(checkedStaged); setCheckedStaged(new Set()); }}
                          className="px-2.5 py-1 bg-green-50 hover:bg-green-100 text-green-700 rounded text-[10px] font-medium border border-green-200">
                          Add {checkedStaged.size} to Database
                        </button>
                        <button onClick={() => { bulkDismiss(checkedStaged); setCheckedStaged(new Set()); }}
                          className="px-2.5 py-1 bg-red-50 hover:bg-red-100 text-red-600 rounded text-[10px] font-medium border border-red-200">
                          Dismiss {checkedStaged.size}
                        </button>
                      </>
                    )}
                  </div>
                </div>
                <div className="flex gap-1.5 text-[10px]">
                  {['Strong', 'Moderate', 'Weak'].map(fit => {
                    const cnt = stagedCompanies.filter(c => c.icpFit === fit).length;
                    if (!cnt) return null;
                    return <span key={fit} className={`px-1.5 py-0.5 rounded ${fit === 'Strong' ? 'bg-green-100 text-green-700' : fit === 'Moderate' ? 'bg-amber-100 text-amber-700' : 'bg-red-100 text-red-600'}`}>{fit}: {cnt}</span>;
                  })}
                </div>
              </div>
              <div className="flex-1 overflow-auto">
                {stagedCompanies.length === 0 ? (
                  <div className="text-center py-16 text-gray-400 text-sm">No screened accounts waiting for review. Screen companies from Discover or Screen Queue.</div>
                ) : (
                  stagedCompanies.map((c, i) => {
                    const idx = companies.indexOf(c);
                    const isSelected = selectedStagedCompany === idx;
                    const isChecked = checkedStaged.has(c.domain);
                    return (
                      <div key={c.domain} onClick={() => setSelectedStagedCompany(idx)}
                        className={`flex items-center gap-3 px-4 py-3 border-b border-gray-100 cursor-pointer transition-colors ${isSelected ? 'bg-violet-50' : 'hover:bg-gray-50'}`}>
                        <div onClick={e => { e.stopPropagation(); setCheckedStaged(prev => { const n = new Set(prev); if (n.has(c.domain)) n.delete(c.domain); else n.add(c.domain); return n; }); }}
                          className={`w-4 h-4 rounded border flex items-center justify-center text-[10px] flex-shrink-0 ${isChecked ? 'bg-violet-500 border-violet-500 text-white' : 'border-gray-300'}`}>
                          {isChecked && '✓'}
                        </div>
                        <div className="flex-1 min-w-0">
                          <div className="text-sm font-medium text-gray-900 truncate">{c.companyName}</div>
                          <div className="text-[10px] text-gray-400 truncate">{c.domain}</div>
                        </div>
                        <div className="flex items-center gap-2 flex-shrink-0">
                          <span className={`font-bold text-sm ${c.totalScore >= 16 ? 'text-green-600' : c.totalScore >= 11 ? 'text-amber-600' : 'text-red-600'}`}>{c.totalScore}/21</span>
                          {c.icpFit && <span className={`px-1.5 py-0.5 rounded text-[9px] border ${
                            c.icpFit === 'Strong' ? 'bg-green-50 text-green-700 border-green-200' :
                            c.icpFit === 'Moderate' ? 'bg-amber-50 text-amber-700 border-amber-200' :
                            c.icpFit === 'Disqualified' ? 'bg-gray-100 text-gray-500 border-gray-200' :
                            'bg-red-50 text-red-600 border-red-200'
                          }`}>{c.icpFit}</span>}
                        </div>
                      </div>
                    );
                  })
                )}
              </div>
            </div>

            {/* Right: detail */}
            {selectedStagedCompany !== null && (() => {
              const c = companies[selectedStagedCompany];
              if (!c || c.dbStatus !== 'screened') return null;
              return (
                <div className="flex-1 bg-white border border-gray-200 rounded-lg overflow-auto">
                  {/* Header */}
                  <div className="px-5 py-4 border-b border-gray-100 sticky top-0 bg-white/95 backdrop-blur z-10">
                    <div className="flex items-center justify-between">
                      <div>
                        <h2 className="text-lg font-bold text-gray-900">{c.companyName}</h2>
                        <div className="text-xs text-gray-500 mt-0.5">
                          <a href={c.website} target="_blank" rel="noopener noreferrer" className="text-violet-600 hover:text-violet-700">{c.domain}</a>
                          <span className="mx-2">·</span>
                          <span className={`font-medium ${c.totalScore >= 16 ? 'text-green-600' : c.totalScore >= 11 ? 'text-amber-600' : 'text-red-600'}`}>{c.totalScore}/21</span>
                          {c.icpFit && <span className={`ml-2 px-1.5 py-0.5 rounded text-[10px] border ${
                            c.icpFit === 'Strong' ? 'bg-green-50 text-green-700 border-green-200' :
                            c.icpFit === 'Moderate' ? 'bg-amber-50 text-amber-700 border-amber-200' :
                            'bg-red-50 text-red-600 border-red-200'
                          }`}>{c.icpFit}</span>}
                        </div>
                      </div>
                      <div className="flex items-center gap-2">
                        <button onClick={() => addToDatabase(c.domain)}
                          className="px-3 py-1.5 bg-green-600 hover:bg-green-500 text-white rounded-lg text-xs font-medium">Add to Database</button>
                        <button onClick={() => { dismissCompany(c.domain); setSelectedStagedCompany(null); }}
                          className="px-3 py-1.5 bg-red-50 hover:bg-red-100 text-red-600 rounded-lg text-xs font-medium border border-red-200">Dismiss</button>
                        <button onClick={() => setSelectedStagedCompany(null)} className="text-gray-500 hover:text-gray-900 text-lg">×</button>
                      </div>
                    </div>
                  </div>

                  {/* Screening Warnings */}
                  {c.screeningWarnings && c.screeningWarnings.length > 0 && (
                    <div className="px-5 py-3 border-b border-amber-100 bg-amber-50/30">
                      <details>
                        <summary className="flex items-center gap-2 cursor-pointer select-none">
                          <span className="text-[10px] text-amber-600 uppercase tracking-wide font-semibold">⚠ Screening Warnings ({c.screeningWarnings.length})</span>
                        </summary>
                        <div className="mt-2 space-y-1">
                          {c.screeningWarnings.map((w, i) => (
                            <div key={i} className="text-[11px] text-amber-700 flex items-start gap-1.5">
                              <span className="text-amber-400 mt-0.5">•</span>
                              <span>{w}</span>
                            </div>
                          ))}
                        </div>
                      </details>
                    </div>
                  )}

                  {/* Top Gaps */}
                  {(c.gap1Opportunity || c.gap2Opportunity) && (
                    <div className="px-5 py-4 border-b border-gray-100 space-y-3">
                      {c.gap1Opportunity && (
                        <div className="p-3 bg-violet-50/50 rounded-lg border border-violet-200/50">
                          <div className="flex items-center gap-2 mb-1.5">
                            <span className="text-[10px] bg-violet-600 text-white px-1.5 py-0.5 rounded font-bold">①</span>
                            <span className="text-xs text-violet-700 font-semibold">{c.gap1Factor}. {c.gap1Name}</span>
                            <span className="text-[10px] text-violet-500">+{c.gap1Score}</span>
                          </div>
                          <div className="text-xs text-gray-600 leading-relaxed">{c.gap1Opportunity}</div>
                        </div>
                      )}
                      {c.gap2Opportunity && (
                        <div className="p-3 bg-violet-50/30 rounded-lg border border-violet-200/30">
                          <div className="flex items-center gap-2 mb-1.5">
                            <span className="text-[10px] bg-violet-400 text-white px-1.5 py-0.5 rounded font-bold">②</span>
                            <span className="text-xs text-violet-600 font-semibold">{c.gap2Factor}. {c.gap2Name}</span>
                            <span className="text-[10px] text-violet-400">+{c.gap2Score}</span>
                          </div>
                          <div className="text-xs text-gray-600 leading-relaxed">{c.gap2Opportunity}</div>
                        </div>
                      )}
                    </div>
                  )}

                  {/* Scoring breakdown — reuse same pattern as Accounts */}
                  {c.scoringResult && (
                    <div className="px-5 py-4 border-b border-gray-100">
                      <div className="text-[9px] text-gray-500 uppercase tracking-wider font-semibold mb-3">Scoring Breakdown</div>
                      <div className="space-y-4">
                        {(() => {
                          const allFactors = [
                            { key: 'A', label: 'A. Differentiation', score: c.scoreA, just: c.scoreAJust, color: 'text-purple-400', borderColor: 'border-purple-500/20' },
                            { key: 'B', label: 'B. Outcomes', score: c.scoreB, just: c.scoreBJust, color: 'text-rose-600', borderColor: 'border-rose-500/20' },
                            { key: 'C', label: 'C. Customer-centric', score: c.scoreC, just: c.scoreCJust, color: 'text-orange-400', borderColor: 'border-orange-500/20' },
                            { key: 'D', label: 'D. Product change', score: c.scoreD, just: c.scoreDJust, color: 'text-cyan-400', borderColor: 'border-cyan-500/20' },
                            { key: 'E', label: 'E. Audience change', score: c.scoreE, just: c.scoreEJust, color: 'text-sky-600', borderColor: 'border-sky-500/20' },
                            { key: 'F', label: 'F. Multi-product', score: c.scoreF, just: c.scoreFJust, color: 'text-violet-600', borderColor: 'border-violet-500/20' },
                            { key: 'G', label: 'G. Vision Gap', score: c.scoreG, just: c.scoreGJust, color: 'text-pink-600', borderColor: 'border-pink-500/20' },
                          ];
                          const g1 = c.gap1Factor ? allFactors.find(f => f.key === c.gap1Factor) : null;
                          const g2 = c.gap2Factor ? allFactors.find(f => f.key === c.gap2Factor) : null;
                          const topKeys = new Set([c.gap1Factor, c.gap2Factor].filter(Boolean));
                          const rest = allFactors.filter(f => !topKeys.has(f.key));
                          const ordered = [...(g1 ? [g1] : []), ...(g2 ? [g2] : []), ...rest];
                          return ordered.map(({ key, label, score, just, color, borderColor }) => {
                            const isTop = topKeys.has(key);
                            const gapNum = key === c.gap1Factor ? '①' : key === c.gap2Factor ? '②' : null;
                            return (
                              <div key={key}>
                                <div className="flex items-center gap-2 mb-1.5">
                                  {gapNum && <span className="text-[10px] bg-violet-600 text-white px-1 py-0.5 rounded font-bold leading-none">{gapNum}</span>}
                                  <span className={`${color} font-semibold text-xs`}>{label}</span>
                                  <span className="flex gap-0.5">
                                    {[1,2,3].map(n => (
                                      <span key={n} className={`inline-block w-3.5 h-1.5 rounded-full ${n <= score ? (score === 3 ? 'bg-green-400' : score === 2 ? 'bg-amber-400' : 'bg-red-400') : 'bg-gray-200'}`} />
                                    ))}
                                  </span>
                                  <span className="text-gray-500 text-[10px]">+{score}</span>
                                </div>
                                <div className={`p-2.5 bg-gray-50/60 rounded-md border ${isTop ? 'border-violet-300/40' : borderColor}`}>
                                  <FactorPanel factorKey={key} data={just} />
                                </div>
                              </div>
                            );
                          });
                        })()}
                      </div>
                    </div>
                  )}

                  {/* Research Report */}
                  <div className="px-5 py-4">
                    <ResearchReport company={c} />
                  </div>
                </div>
              );
            })()}

            {selectedStagedCompany === null && (
              <div className="flex-1 bg-white border border-gray-200 rounded-lg flex items-center justify-center">
                <div className="text-center text-gray-400 text-sm">Select a company to review its scoring and research</div>
              </div>
            )}
          </div>
        )}

        {/* ======= EMAIL GENERATION VIEW (placeholder) ======= */}
        {activeView === 'email_gen' && (
          <div className="bg-white border border-gray-200 rounded-lg p-8" style={{ minHeight: '60vh' }}>
            <h2 className="text-lg font-bold text-gray-900 mb-2">Email Generation</h2>
            <p className="text-sm text-gray-500 mb-4">Generate and refine outreach emails using AI. Select a campaign and contact to get started.</p>
            <div className="text-center py-16 text-gray-400 text-sm">
              Coming soon. Email generation controls will be moved here from the Campaigns view.
            </div>
          </div>
        )}

        {/* ======= PROMPT SETTINGS VIEW (placeholder) ======= */}
        {activeView === 'prompt_settings' && (
          <div className="space-y-6">
            {/* Scoring Prompts */}
            <div className="bg-white border border-gray-200 rounded-lg p-6">
              <h2 className="text-lg font-bold text-gray-900 mb-1">Scoring Prompts</h2>
              <p className="text-sm text-gray-500 mb-4">Edit the prompts used for each scoring factor. Changes are saved automatically and persist across page refreshes.</p>
              <div className="space-y-4">
                {[
                  { key: 'system', label: 'System Prompt (shared)', desc: 'Sent as the system message for all 8 scoring calls', rows: 3 },
                  { key: 'disqualification', label: 'Disqualification Check', desc: 'Determines if a company should be excluded from scoring', rows: 6 },
                  { key: 'A', label: 'Factor A — Differentiation', desc: 'Evaluates unique customer benefits vs homepage communication', rows: 10 },
                  { key: 'B', label: 'Factor B — Outcomes', desc: 'Evaluates key KPIs vs homepage prominence', rows: 10 },
                  { key: 'C', label: 'Factor C — Customer-Centric', desc: 'Evaluates buyer perspective vs product-as-hero framing', rows: 10 },
                  { key: 'D', label: 'Factor D — Product Change', desc: 'Evaluates product evolution vs homepage alignment', rows: 10 },
                  { key: 'E', label: 'Factor E — Audience Change', desc: 'Evaluates target buyer shifts with confidence rating', rows: 10 },
                  { key: 'F', label: 'Factor F — Multi-Product', desc: 'Evaluates narrative coherence across products', rows: 8 },
                  { key: 'G', label: 'Factor G — Vision Gap', desc: 'Evaluates CEO narrative vs homepage narrative', rows: 8 },
                ].map(({ key, label, desc, rows }) => (
                  <div key={key} className="border border-gray-200 rounded-lg overflow-hidden">
                    <div className="flex items-center justify-between px-4 py-2.5 bg-gray-50 border-b border-gray-200">
                      <div>
                        <div className="text-sm font-semibold text-gray-800">{label}</div>
                        <div className="text-[10px] text-gray-400">{desc}</div>
                      </div>
                      <div className="flex items-center gap-2">
                        {scoringPrompts[key] !== ({ system: SYSTEM_PROMPT_SCORER, disqualification: DISQUALIFICATION_PROMPT, A: FACTOR_A_PROMPT, B: FACTOR_B_PROMPT, C: FACTOR_C_PROMPT, D: FACTOR_D_PROMPT, E: FACTOR_E_PROMPT, F: FACTOR_F_PROMPT, G: FACTOR_G_PROMPT })[key] && (
                          <span className="text-[9px] text-amber-600 bg-amber-50 px-1.5 py-0.5 rounded">Modified</span>
                        )}
                        <button onClick={() => resetScoringPrompt(key)}
                          className="text-[10px] text-gray-400 hover:text-red-500">Reset to Default</button>
                      </div>
                    </div>
                    <textarea value={scoringPrompts[key]} onChange={e => updateScoringPrompt(key, e.target.value)}
                      rows={rows}
                      className="w-full px-4 py-3 text-xs text-gray-700 font-mono leading-relaxed border-0 focus:outline-none focus:ring-1 focus:ring-violet-300 resize-y" />
                  </div>
                ))}
              </div>
            </div>

            {/* Training Examples */}
            <div className="bg-white border border-gray-200 rounded-lg p-6">
              <h2 className="text-lg font-bold text-gray-900 mb-1">Training Examples</h2>
              <p className="text-sm text-gray-500 mb-4">Manual email examples used as few-shot demonstrations during email generation.</p>
              <div className="flex gap-2 mb-4">
                {['A','B','C','D','E','F','G'].map(f => {
                  const names = { A: 'Differentiation', B: 'Outcomes', C: 'Customer-centric', D: 'Product Change', E: 'Audience Change', F: 'Multi-product', G: 'Vision Gap' };
                  const count = trainingExamples.filter(e => e.gap_factor === f).length;
                  return (
                    <div key={f} className={`px-3 py-2 rounded-lg border text-center min-w-[80px] ${count >= 3 ? 'bg-green-50 border-green-200' : count >= 1 ? 'bg-amber-50 border-amber-200' : 'bg-gray-50 border-gray-200'}`}>
                      <div className="text-xs font-semibold text-gray-700">{f}</div>
                      <div className="text-[10px] text-gray-500">{names[f]}</div>
                      <div className={`text-sm font-bold mt-1 ${count >= 3 ? 'text-green-600' : count >= 1 ? 'text-amber-600' : 'text-gray-400'}`}>{count}</div>
                    </div>
                  );
                })}
              </div>
              {trainingExamples.length > 0 ? (
                <div className="space-y-2">
                  {trainingExamples.map(ex => (
                    <div key={ex.id} className="p-3 bg-gray-50 rounded-lg border border-gray-200">
                      <div className="flex items-center gap-2 mb-1">
                        <span className="px-1.5 py-0.5 bg-violet-100 text-violet-700 rounded text-[9px] font-semibold">{ex.gap_factor}. {ex.gap_name}</span>
                        <span className="text-gray-300 text-[10px]">{ex.company_name}</span>
                        <button onClick={async () => { await deleteTrainingExample(ex.id); setTrainingExamples(prev => prev.filter(e => e.id !== ex.id)); }}
                          className="ml-auto text-gray-300 hover:text-red-500 text-xs">Delete</button>
                      </div>
                      {ex.context && <div className="text-[10px] text-violet-500 mb-1">Context: {ex.context}</div>}
                      <div className="text-[11px] text-gray-500 whitespace-pre-wrap leading-relaxed mt-1">{ex.opening || ex.body || ''}</div>
                      {ex.avoid_notes && <div className="mt-1.5 text-[10px] text-amber-600">⚠ Avoid: {ex.avoid_notes}</div>}
                    </div>
                  ))}
                </div>
              ) : (
                <div className="text-center py-8 text-gray-400 text-sm">No training examples yet. Write manual emails in the Campaign tab to create examples.</div>
              )}
            </div>
          </div>
        )}

      </div>
    </div>
  );
}
