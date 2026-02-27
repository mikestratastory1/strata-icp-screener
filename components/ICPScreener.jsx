import React, { useState, useRef, useEffect, useCallback } from 'react';
import { supabase, normalizeDomain, upsertCompany as dbUpsertCompany, createResearchRun, updateResearchRun, getCompaniesWithLatest, updateCompany as dbUpdateCompany, saveTrainingExample, getTrainingExamples, deleteTrainingExample, upsertContacts, upsertContact, updateContact, getAllContacts, getContactsByCompany, deleteContact, deleteCompany, getAllCampaigns, createCampaign, updateCampaign, deleteCampaign, getCampaignContacts, getAllCampaignContacts, addContactsToCampaign, removeContactFromCampaign, getCampaignMessages, createCampaignMessage, upsertCampaignMessage, deleteCampaignMessage, getSavedFilters, createSavedFilter, updateSavedFilter, deleteSavedFilter } from '../lib/supabase';

const RESEARCH_PROMPT = `You are a B2B research analyst synthesizing pre-gathered data about a company. All the raw data from web searches, homepage crawls, news articles, competitor reviews, case studies, and social content has already been collected and is provided below. Your job is to organize this data into a structured research report. Do NOT make up information — only use what is in the provided data. If data for a field is missing, say so.

=== SECTION A: STRATEGIC RESEARCH ===

PRODUCT_SUMMARY: [What does the product do in 2-3 sentences. Not marketing language - describe it plainly like you're explaining to a colleague. Include the target market and key use cases.]

TARGET_CUSTOMER: [Who buys this? Company size (SMB/mid-market/enterprise), industries, geographic focus. Be specific - "Series B+ SaaS companies" not just "businesses." Use case studies, homepage logos, and LinkedIn description as evidence.]

TARGET_DECISION_MAKER: [Who is the primary buyer - the person who signs the contract, not the user. Look at case study data for who is quoted. Then infer ONE LEVEL UP to the budget holder. Example: if a Director of Content Marketing is quoted, the decision maker is the CMO or VP of Marketing. State the inferred decision maker title.]

TOP_3_OUTCOMES: [The 3 most specific outcomes this product helps the decision maker's team achieve. Must be outcomes with numbers/metrics when available. Example: "Reduce no-shows by 75%" not "appointment reminders." Pull from case studies and news data.]

TOP_3_DIFFERENTIATORS: [What 3 things make this company different from competitors? Use the competitor comparison data and reviews. IMPORTANT: deprioritize "easy to use", "great UX", "simple interface" — these are not meaningful differentiators. Look for capability themes, unique approaches, integration advantages, or specific outcomes. Each differentiator should pass the test: "Could a competitor also say this?" If yes, it's not a differentiator.]

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


const SCORING_PROMPT = `You are scoring a B2B SaaS company for narrative gap severity. CRITICAL: You MUST respond with a single JSON object. Do NOT use the old SCORE_A_DIFFERENTIATION/SCORE_A_JUSTIFICATION text format. Return ONLY a JSON object as specified below.

You have been given comprehensive research about this company. Your job is to evaluate the narrative using an additive scoring system. Do not gather more information.

=== SCORING SYSTEM ===

Score each factor 1, 2, or 3. Higher = more pain = better ICP fit. Total score = sum of all 6 factors (range: 6-18).

=== FACTOR INSTRUCTIONS ===

FACTOR A — DIFFERENTIATION: Compare TOP_3_DIFFERENTIATORS + COMPETITORS vs HOMEPAGE_SECTIONS. Hero carries most weight.
+1: Hero or Section 2 communicates a specific capability competitors don't claim
+2: Some differentiation exists but generic, buried, or could apply to competitors  
+3: Homepage could belong to any competitor

FACTOR B — OUTCOMES: Compare TOP_3_OUTCOMES vs HOMEPAGE_SECTIONS. Strategic = exec priorities (cost, revenue, risk, margin). Tactical = operational (time saved, tasks automated).
+1: Homepage prominently features quantified strategic outcomes in Sections 1-2
+2: Outcomes exist but tactical, buried, or lack metrics
+3: Dominated by features — outcomes absent or vague

FACTOR C — CUSTOMER-CENTRIC: Check who is the grammatical subject in HOMEPAGE_SECTIONS. Evaluate company-authored copy ONLY — exclude testimonial/quote sections.
+1: Hero frames value from buyer's perspective — buyer is the subject
+2: Mixed — some buyer language but hero defaults to product descriptions
+3: Homepage primarily about the product/company — buyer's world secondary

FACTOR D — PRODUCT CHANGE: Read MAJOR_ANNOUNCEMENTS, CEO_RECENT_CONTENT, CEO_NARRATIVE_THEME.
+1: Product stable — no narrative-relevant changes. Minor updates, bug fixes, compliance changes, vendor swaps. Incremental feature releases that don't alter the core value prop. Partnerships that don't change what the company offers. Test: Would a prospect's understanding of the product change? If no → 1.
+2: Product expanding — value prop is stretching. New module or capability that adds a meaningful use case. Acquisition or partnership that extends what the platform can do. Test: Would the "what we do" section of a pitch need updating? If yes → 2.
+3: Product transforming — the company story needs rewriting. Core offering has fundamentally shifted (new primary product, pivot, rebrand). Company is solving a materially different problem than 12 months ago. Test: Would someone who visited the homepage a year ago be confused by what the company is now? If yes → 3.

FACTOR E — AUDIENCE CHANGE: Read MAJOR_ANNOUNCEMENTS, CEO content, TARGET_CUSTOMER, PRODUCT_PAGES.
+1: Buyer and market consistent 12+ months
+2: Expanding into adjacent segment or secondary persona
+3: Meaningful shift in who they sell to

FACTOR F — MULTI-PRODUCT: Read PRODUCT_PAGES, HOMEPAGE_NAVIGATION, HOMEPAGE_SECTIONS.
+1: Single product or tightly integrated suite, unified narrative
+2: Multiple products but homepage connects them under one story
+3: Products have different audiences/value props — feels fragmented

=== DISQUALIFICATION FLAGS ===
Check first. If any apply, set icp_fit to "Disqualified":
- Acquired by larger company (not independent)
- Consumer product, not B2B SaaS
- Crypto/Web3/prediction markets
- Pre-product or research-phase

=== CALIBRATION ===
- "1" = basics covered, "3" = not doing the job
- Anchor to SECTION NUMBER. Outcome in Section 4 = score 2, not 1.
- CEO voice is a tiebreaker for D and E.
- Well-funded company with generic homepage = bigger gap than seed-stage.

=== OUTPUT FORMAT ===

Return ONLY valid JSON (no markdown, no backticks, no text before/after). Use this exact structure:

{
  "total_score": 12,
  "icp_fit": "Moderate",
  "disqualification_reason": "None",
  "summary": "2-3 sentence summary of company achievement and narrative gap.",
  "factor_a": {
    "score": 3,
    "differentiators": [
      "First differentiator from research",
      "Second differentiator from research",
      "Third differentiator from research"
    ],
    "homepage_sections": [
      { "name": "Hero", "finding": "Quote or description of what hero says about differentiation", "status": "miss" },
      { "name": "Customers", "finding": "What this section says", "status": "miss" },
      { "name": "Product", "finding": "What this section says", "status": "hit" },
      { "name": "Partnership", "finding": "What this section says", "status": "hit" }
    ],
    "verdict": "One sentence verdict explaining the score."
  },
  "factor_b": {
    "score": 2,
    "decision_maker": "Title of primary buyer",
    "strategic_outcomes": [
      "Exec-level outcome 1 from research (cost/revenue/risk/margin)",
      "Exec-level outcome 2"
    ],
    "tactical_outcomes": [
      "Operational outcome 1 (time saved, productivity)",
      "Operational outcome 2"
    ],
    "homepage_sections": [
      { "name": "Hero", "finding": "What the hero says about outcomes", "outcome_type": "none" },
      { "name": "Customers", "finding": "Quote with metric if present", "outcome_type": "tactical" },
      { "name": "Product", "finding": "Quote or description", "outcome_type": "tactical" },
      { "name": "Partnership", "finding": "Description", "outcome_type": "none" }
    ],
    "verdict": "One sentence verdict."
  },
  "factor_c": {
    "score": 3,
    "sections": [
      { "name": "Hero", "orientation": "product-centric", "evidence": "Quote showing grammatical subject" },
      { "name": "Customers", "orientation": "excluded", "evidence": "Testimonial quotes — buyer's words" },
      { "name": "Product", "orientation": "product-centric", "evidence": "Quote showing subject" },
      { "name": "Partnership", "orientation": "product-centric", "evidence": "Quote showing subject" }
    ],
    "verdict": "One sentence verdict."
  },
  "factor_d": {
    "score": 2,
    "changes": [
      {
        "date": "Mid 2024",
        "name": "Change name",
        "before": "What the product/value prop was before",
        "after": "What it became after"
      }
    ],
    "verdict": "One sentence verdict. Note if homepage reflects changes or not."
  },
  "factor_e": {
    "score": 1,
    "before": { "buyer": "Title", "department": "Dept", "market": "Market segment" },
    "today": { "buyer": "Title", "department": "Dept", "market": "Market segment" },
    "verdict": "One sentence verdict."
  },
  "factor_f": {
    "score": 1,
    "products": [
      { "name": "Product or module name", "tag": "module" }
    ],
    "description": "Brief description of product architecture.",
    "verdict": "One sentence verdict."
  }
}

CRITICAL RULES:
- Return ONLY the JSON object. No text before or after. No markdown code fences.
- homepage_sections: Include the first 4 significant content sections (skip nav, footer, logo bars). Label each with 1-2 word name.
- status values for factor_a: "hit" (differentiator found) or "miss" (absent/generic)
- outcome_type values for factor_b: "strategic", "tactical", or "none"
- orientation values for factor_c: "product-centric", "customer-centric", "mixed", or "excluded" (for testimonial sections)
- For factor_d changes array: empty array [] if no changes. Include date as "Month YYYY" or "Early/Mid/Late YYYY".
- For factor_f tag values: "module" (capability within one product), "product" (distinct product), or "suite" (separate product line)
- All string values must be properly escaped for JSON.

REMEMBER: Output ONLY the JSON object. No text before it. No text after it. No markdown fences. Start your response with { and end with }.`;

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
          <Lbl>Decision Maker & Outcome Priorities</Lbl>
          {d.decision_maker && <div className="text-gray-400 mb-1 font-semibold">{d.decision_maker}</div>}
          {(d.strategic_outcomes || []).length > 0 && (
            <>
              <div className="text-rose-600 font-medium text-[10px] mb-0.5">STRATEGIC (exec-level)</div>
              <div className="flex flex-col gap-px text-gray-400 mb-1.5">
                {d.strategic_outcomes.map((o, i) => <div key={i}>• {o}</div>)}
              </div>
            </>
          )}
          {(d.tactical_outcomes || []).length > 0 && (
            <>
              <div className="text-amber-600 font-medium text-[10px] mb-0.5">TACTICAL (operational)</div>
              <div className="flex flex-col gap-px text-gray-500">
                {d.tactical_outcomes.map((o, i) => <div key={i}>• {o}</div>)}
              </div>
            </>
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
        <div className="flex flex-col gap-2 text-[11px]">
          {d.changes.map((ch, i) => (
            <div key={i}>
              <div className="flex items-center gap-1.5 mb-1">
                <span className="text-cyan-400 font-semibold">{ch.name}</span>
                <span className="text-gray-500 text-[10px]">{ch.date}</span>
              </div>
              <div className="grid grid-cols-2 gap-2">
                <div className="p-1.5 bg-gray-100/80 rounded border border-gray-700/50">
                  <div className="text-[9px] text-gray-500 font-semibold mb-px">BEFORE</div>
                  <div className="text-gray-500">{ch.before}</div>
                </div>
                <div className="p-1.5 bg-cyan-950/30 rounded border border-cyan-800/30">
                  <div className="text-[9px] text-cyan-400 font-semibold mb-px">AFTER</div>
                  <div className="text-gray-400">{ch.after}</div>
                </div>
              </div>
            </div>
          ))}
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
      {d.description && <div className="text-[11px] text-gray-400 mb-1">{d.description}</div>}
      {(d.products || []).length > 0 && (
        <div className="flex gap-1 flex-wrap mb-1">
          {d.products.map((p, i) => (
            <Tag key={i} color={tagColors[p.tag] || 'gray'}>{p.name}</Tag>
          ))}
        </div>
      )}
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

const PANEL_MAP = { A: PanelA, B: PanelB, C: PanelC, D: PanelD, E: PanelE, F: PanelF };

function FactorPanel({ factorKey, data }) {
  if (!data) return null;
  const d = tryParseJSON(data);
  if (d && typeof d === 'object' && (d.verdict || d.sections || d.differentiators || d.changes || d.products || d.before || d.homepage_sections || d.strategic_outcomes || d.tactical_outcomes || d.decision_maker || d.description)) {
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

export default function ICPScreener() {
  const [companies, setCompanies] = useState([]);
  const [isProcessing, setIsProcessing] = useState(false);
  const [error, setError] = useState('');
  const [debugLog, setDebugLog] = useState([]);
  const [selectedCompany, setSelectedCompany] = useState(null);
  const [showCsvModal, setShowCsvModal] = useState(false);
  const [csvContent, setCsvContent] = useState('');
  const [concurrency, setConcurrency] = useState(2);
  const [activeView, setActiveView] = useState('discover'); // 'discover' | 'upload' | 'accounts' | 'contacts'
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
    // Funding
    { key: 'last_funding_round_type', label: 'Funding Stage', category: 'Funding', inputType: 'multi_select', operators: ['in', 'not_in'], defaultOp: 'in', options: ['seed', 'pre_seed', 'angel', 'series_a', 'series_b', 'series_c', 'series_d', 'series_e', 'series_f', 'series_g', 'series_h', 'debt_financing', 'convertible_note', 'grant', 'private_equity', 'secondary_market', 'undisclosed'] },
    { key: 'last_funding_date', label: 'Last Funding Date', category: 'Funding', inputType: 'date', operators: ['=>', '=<', '>', '<'], defaultOp: '=>' },
    { key: 'crunchbase_total_investment_usd', label: 'Total Funding ($)', category: 'Funding', inputType: 'number', operators: ['>', '<', '=>', '=<'], defaultOp: '>' },
    { key: 'crunchbase_investors', label: 'Investors', category: 'Funding', inputType: 'autocomplete_multi', operators: ['in'], defaultOp: 'in', autocompleteField: 'crunchbase_investors' },
    { key: 'tracxn_investors', label: 'Tracxn Investors', category: 'Funding', inputType: 'autocomplete_multi', operators: ['in'], defaultOp: 'in', autocompleteField: 'tracxn_investors' },
    // Location
    { key: 'hq_country', label: 'HQ Country', category: 'Location', inputType: 'autocomplete_multi', operators: ['=', 'in', 'not_in'], defaultOp: 'in', autocompleteField: 'hq_country' },
    { key: 'hq_location', label: 'HQ Location', category: 'Location', inputType: 'autocomplete_text', operators: ['(.)', '[.]', '='], defaultOp: '(.)', autocompleteField: 'hq_location' },
    { key: 'largest_headcount_country', label: 'Largest Headcount Country', category: 'Location', inputType: 'autocomplete_multi', operators: ['=', 'in', '!='], defaultOp: 'in', autocompleteField: 'largest_headcount_country' },
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
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  // Load saved filters on mount
  useEffect(() => {
    if (supabase) { getSavedFilters().then(setSavedFilters).catch(() => {}); }
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
  const [contactsModal, setContactsModal] = useState(false);
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
  const [titleSuggestions, setTitleSuggestions] = useState([]);
  const [titleAutocompleteLoading, setTitleAutocompleteLoading] = useState(false);
  
  const FUNCTION_OPTIONS = ['Marketing', 'Sales', 'Engineering', 'Product Management', 'Operations', 'Finance', 'Human Resources', 'Design', 'Customer Success', 'Business Development'];
  const [enrichingContacts, setEnrichingContacts] = useState({}); // { contactId: 'loading' | 'done' | 'error' }

  // Campaign state
  const [campaigns, setCampaigns] = useState([]);
  const [activeCampaignId, setActiveCampaignId] = useState(null);
  const [campaignContacts, setCampaignContacts] = useState([]);
  const [allCampaignContacts, setAllCampaignContacts] = useState([]); // all campaign_contacts for filtering
  const [campaignMessages, setCampaignMessages] = useState([]);
  const [selectedCampaignContact, setSelectedCampaignContact] = useState(null);
  const [checkedContacts, setCheckedContacts] = useState(new Set()); // for contacts tab bulk actions
  const [checkedAccounts, setCheckedAccounts] = useState(new Set()); // for accounts tab bulk actions (domains)
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
              tactical_outcomes: (r.b_tactical_outcomes || '').split('; ').filter(Boolean),
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
              icpFit: r.icp_fit === 'Disqualified' ? 'Disqualified' : ((r.total_score || 0) >= 14 ? 'Strong' : (r.total_score || 0) >= 10 ? 'Moderate' : (r.total_score || 0) >= 6 ? 'Weak' : (r.icp_fit || '')),
              disqualificationReason: r.disqualification_reason || '',
              aVerdict: r.a_verdict || '', bVerdict: r.b_verdict || '', cVerdict: r.c_verdict || '',
              dVerdict: r.d_verdict || '', eVerdict: r.e_verdict || '', fVerdict: r.f_verdict || '',
              manualScore: r.manual_score || '',
              accountStatus: r.account_status || 'Cold',
              lastScreenedAt: r.last_screened_at || r.run_created_at || null,
              addedAt: r.created_at || null,
              status: r.run_status || (r.run_id ? 'complete' : 'pending'), step: '', error: r.run_error || null
            };
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
        scoreSummary: '',
        icpFit: '',
        disqualificationReason: '',
        manualScore: manualScoreIdx !== -1 ? (values[manualScoreIdx] || '').trim() : '',
        accountStatus: 'Cold',
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
              const dbCo = await dbUpsertCompany(domain, co.companyName, co.website);
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
    const fieldPattern = new RegExp(`${field}:\\s*([\\s\\S]*?)(?=\\n[A-Z_]+:|$)`, 'i');
    const match = text.match(fieldPattern);
    return match ? match[1].trim() : '';
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
        const dbCo = await dbUpsertCompany(domain, companyName, website);
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

      // STEP 1b: SYNTHESIZE WITH CLAUDE (no web search needed)
      updateCompany({ step: 'Step 1b: Synthesizing research...' });
      addLog(`[${companyName}] Step 1b: Synthesis (Haiku, no web search)`);
      const researchPrompt = `Synthesize the following pre-gathered research data into a structured report.\n\nCompany: ${companyName}\nWebsite: ${website}\n\n${exaContext}\n\n${RESEARCH_PROMPT}`;
      const researchResult = await callClaude(researchPrompt, false, "claude-haiku-4-5-20251001");
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
      
      // Build training examples section if any exist
      let trainingSection = '';
      if (trainingExamples.length > 0) {
        const factorNames = { A: 'DIFFERENTIATION', B: 'OUTCOMES', C: 'CUSTOMER_CENTRIC', D: 'PRODUCT_CHANGE', E: 'AUDIENCE_CHANGE', F: 'MULTI_PRODUCT' };
        trainingSection = '\n\n=== CALIBRATION EXAMPLES ===\nBelow are manually reviewed and corrected scoring examples for individual factors. Use these to calibrate your scoring. Match the reasoning style and score levels shown here.\n\n';
        // Group by company
        const byCompany = {};
        for (const ex of trainingExamples) {
          if (!byCompany[ex.domain]) byCompany[ex.domain] = { name: ex.company_name, factors: {}, snapshot: ex.research_snapshot };
          byCompany[ex.domain].factors[ex.factor] = { score: ex.score, justification: ex.justification };
        }
        for (const [domain, { name, factors, snapshot }] of Object.entries(byCompany)) {
          trainingSection += `--- ${name} (${domain}) ---\n`;
          trainingSection += `Research (abbreviated):\n${(snapshot || '').slice(0, 1500)}\n\n`;
          trainingSection += `CORRECT SCORES:\n`;
          for (const [f, { score, justification }] of Object.entries(factors)) {
            trainingSection += `SCORE_${f}_${factorNames[f]}: ${score}\nSCORE_${f}_JUSTIFICATION: ${justification}\n`;
          }
          trainingSection += '\n';
        }
        trainingSection += '=== END CALIBRATION EXAMPLES ===\n\nNow score the following company using the same standards:\n';
      }
      
      const scoringPrompt = `Score this company's narrative gap using the research provided.\n\nCompany: ${companyName}\nWebsite: ${website}\n\n=== RESEARCH RESULTS ===\n${researchResult}\n=== END RESEARCH ===\n\n${SCORING_PROMPT}${trainingSection}`;
      const scoringResult = await callClaude(scoringPrompt, false, "claude-sonnet-4-20250514", "You are a JSON-only scoring API. You MUST respond with a single valid JSON object. No markdown, no code fences, no explanatory text. Start your response with { and end with }.");
      
      // Try to parse as JSON first, fall back to field parsing
      let scoringData = null;
      try {
        let jsonStr = scoringResult;
        jsonStr = jsonStr.replace(/^[\s\S]*?```(?:json)?\s*/i, '').replace(/\s*```[\s\S]*$/i, '');
        if (!jsonStr.trim().startsWith('{')) {
          const firstBrace = jsonStr.indexOf('{');
          const lastBrace = jsonStr.lastIndexOf('}');
          if (firstBrace !== -1 && lastBrace !== -1) {
            jsonStr = jsonStr.slice(firstBrace, lastBrace + 1);
          }
        }
        scoringData = JSON.parse(jsonStr.trim());
        addLog(`[${companyName}] Parsed structured JSON scoring`);
      } catch (e) {
        addLog(`[${companyName}] JSON parse failed (${e.message}), using field parser`);
      }
      
      let totalScore, icpFit, scoreFields;
      if (scoringData) {
        totalScore = scoringData.total_score || 0;
        icpFit = totalScore >= 14 ? 'Strong' : totalScore >= 10 ? 'Moderate' : 'Weak';
        // Disqualification from model overrides score-based fit
        if (scoringData.icp_fit === 'Disqualified' || (scoringData.disqualification_reason && scoringData.disqualification_reason !== 'None' && scoringData.disqualification_reason !== 'none')) {
          icpFit = 'Disqualified';
        }
        
        const fa = scoringData.factor_a || {};
        const fb = scoringData.factor_b || {};
        const fc = scoringData.factor_c || {};
        const fd = scoringData.factor_d || {};
        const fe = scoringData.factor_e || {};
        const ff = scoringData.factor_f || {};
        
        // Extract homepage section names from factor_a (they're shared across A, B, C)
        const aSections = fa.homepage_sections || [];
        const sectionNames = aSections.map(s => s.name || '');
        
        // Factor B and C sections for cross-reference
        const bSections = fb.homepage_sections || [];
        const cSections = fc.sections || [];
        
        // Format changes for D
        const dChanges = (fd.changes || []).map(ch => 
          `${ch.name} (${ch.date}): ${ch.before} → ${ch.after}`
        ).join('; ');
        
        // Format products for F
        const fProducts = (ff.products || []).map(p => 
          `${p.name} (${p.tag})`
        ).join(', ');
        
        scoreFields = {
          scoringResult, totalScore, icpFit,
          scoreSummary: scoringData.summary || '',
          disqualificationReason: scoringData.disqualification_reason || 'None',
          
          // Keep JSON for panels (in-memory only, not saved to DB)
          scoreAJust: JSON.stringify(fa),
          scoreBJust: JSON.stringify(fb),
          scoreCJust: JSON.stringify(fc),
          scoreDJust: JSON.stringify(fd),
          scoreEJust: JSON.stringify(fe),
          scoreFJust: JSON.stringify(ff),
          
          // Homepage section names
          hpSection1Name: sectionNames[0] || '',
          hpSection2Name: sectionNames[1] || '',
          hpSection3Name: sectionNames[2] || '',
          hpSection4Name: sectionNames[3] || '',
          
          // Factor A broken out
          scoreA: fa.score || 0,
          aDifferentiators: (fa.differentiators || []).join('; '),
          aSection1Finding: aSections[0]?.finding || '', aSection1Status: aSections[0]?.status || '',
          aSection2Finding: aSections[1]?.finding || '', aSection2Status: aSections[1]?.status || '',
          aSection3Finding: aSections[2]?.finding || '', aSection3Status: aSections[2]?.status || '',
          aSection4Finding: aSections[3]?.finding || '', aSection4Status: aSections[3]?.status || '',
          aVerdict: fa.verdict || '',
          
          // Factor B broken out
          scoreB: fb.score || 0,
          bDecisionMaker: fb.decision_maker || '',
          bStrategicOutcomes: (fb.strategic_outcomes || []).join('; '),
          bTacticalOutcomes: (fb.tactical_outcomes || []).join('; '),
          bSection1Finding: bSections[0]?.finding || '', bSection1Type: bSections[0]?.outcome_type || '',
          bSection2Finding: bSections[1]?.finding || '', bSection2Type: bSections[1]?.outcome_type || '',
          bSection3Finding: bSections[2]?.finding || '', bSection3Type: bSections[2]?.outcome_type || '',
          bSection4Finding: bSections[3]?.finding || '', bSection4Type: bSections[3]?.outcome_type || '',
          bVerdict: fb.verdict || '',
          
          // Factor C broken out
          scoreC: fc.score || 0,
          cSection1Orientation: cSections[0]?.orientation || '', cSection1Evidence: cSections[0]?.evidence || '',
          cSection2Orientation: cSections[1]?.orientation || '', cSection2Evidence: cSections[1]?.evidence || '',
          cSection3Orientation: cSections[2]?.orientation || '', cSection3Evidence: cSections[2]?.evidence || '',
          cSection4Orientation: cSections[3]?.orientation || '', cSection4Evidence: cSections[3]?.evidence || '',
          cVerdict: fc.verdict || '',
          
          // Factor D broken out
          scoreD: fd.score || 0,
          dChanges,
          dVerdict: fd.verdict || '',
          
          // Factor E broken out
          scoreE: fe.score || 0,
          eAudienceBefore: fe.before ? `${fe.before.buyer} — ${fe.before.department} — ${fe.before.market}` : '',
          eAudienceToday: fe.today ? `${fe.today.buyer} — ${fe.today.department} — ${fe.today.market}` : '',
          eVerdict: fe.verdict || '',
          
          // Factor F broken out
          scoreF: ff.score || 0,
          fProducts,
          fDescription: ff.description || '',
          fVerdict: ff.verdict || '',
          
          status: 'complete', step: '', error: null
        };
      } else {
        totalScore = parseInt(parseField(scoringResult, 'TOTAL_SCORE'), 10) || 0;
        icpFit = totalScore >= 14 ? 'Strong' : totalScore >= 10 ? 'Moderate' : 'Weak';
        const parsedDisqReason = parseField(scoringResult, 'DISQUALIFICATION_REASON');
        if (parseField(scoringResult, 'ICP_FIT') === 'Disqualified' || (parsedDisqReason && parsedDisqReason !== 'None' && parsedDisqReason !== 'none')) {
          icpFit = 'Disqualified';
        }
        scoreFields = {
          scoringResult, totalScore,
          scoreA: parseInt(parseField(scoringResult, 'SCORE_A_DIFFERENTIATION'), 10) || 0,
          scoreAJust: parseField(scoringResult, 'SCORE_A_JUSTIFICATION'),
          scoreB: parseInt(parseField(scoringResult, 'SCORE_B_OUTCOMES'), 10) || 0,
          scoreBJust: parseField(scoringResult, 'SCORE_B_JUSTIFICATION'),
          scoreC: parseInt(parseField(scoringResult, 'SCORE_C_CUSTOMER_CENTRIC'), 10) || 0,
          scoreCJust: parseField(scoringResult, 'SCORE_C_JUSTIFICATION'),
          scoreD: parseInt(parseField(scoringResult, 'SCORE_D_PRODUCT_CHANGE'), 10) || 0,
          scoreDJust: parseField(scoringResult, 'SCORE_D_JUSTIFICATION'),
          scoreE: parseInt(parseField(scoringResult, 'SCORE_E_AUDIENCE_CHANGE'), 10) || 0,
          scoreEJust: parseField(scoringResult, 'SCORE_E_JUSTIFICATION'),
          scoreF: parseInt(parseField(scoringResult, 'SCORE_F_MULTI_PRODUCT'), 10) || 0,
          scoreFJust: parseField(scoringResult, 'SCORE_F_JUSTIFICATION'),
          scoreSummary: parseField(scoringResult, 'SCORE_SUMMARY'),
          icpFit, disqualificationReason: parseField(scoringResult, 'DISQUALIFICATION_REASON'),
          aVerdict: '', bVerdict: '', cVerdict: '', dVerdict: '', eVerdict: '', fVerdict: '',
          status: 'complete', step: '', error: null
        };
      }
      updateCompany(scoreFields);
      addLog(`[${companyName}] Complete: Score: ${totalScore}/18, Fit: ${icpFit}`);

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
            b_strategic_outcomes: scoreFields.bStrategicOutcomes || '',
            b_tactical_outcomes: scoreFields.bTacticalOutcomes || '',
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
            f_description: scoreFields.fDescription || '',
            f_verdict: scoreFields.fVerdict || '',
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
      
      conditions.push({ filter_type: f.fieldKey, type: f.operator, value });
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
        
        const resp = await fetch('/api/crustdata', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            action: 'search',
            filters,
            sorts: [{ column: 'employee_metrics.latest_count', order: 'desc' }],
            limit: 10,
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
          const industry = Array.isArray(co.linkedin_industries) ? co.linkedin_industries.join(', ')
            : (co.industry || '');
          
          return {
            name: co.company_name || co.name || '',
            domain,
            website,
            industry,
            employees: co.employee_metrics?.latest_count || co.employee_count || 0,
            funding: co.last_funding_round_type || '',
            totalFunding: co.crunchbase_total_investment_usd || 0,
            location: co.hq_location || co.location || '',
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
            location: co.hq_location || co.location || co.hq || '',
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
  
  const addDiscoveredToQueue = (selected = null) => {
    const toAdd = selected ? discoverResults.filter(c => selected.has(c.domain)) : discoverResults;
    const existingDomains = new Set(companies.map(c => c.domain));
    const newCompanies = toAdd
      .filter(c => !existingDomains.has(c.domain))
      .map(c => ({
        companyName: c.name,
        website: c.website,
        domain: c.domain,
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

  const openContactsModal = (company = null) => {
    setContactsCompany(company); // single company {domain, name} or array [{domain, name}, ...]
    setContactsResults([]);
    setContactsError('');
    setContactsCursor(null);
    setContactsTotal(0);
    setSelectedContactResults(new Set());
    setContactsFilters({ titles: '', functions: [], verifiedEmailOnly: false, recentlyChangedJobs: false });
    setTitleSuggestions([]);
    setContactsModal(true);
  };

  // Batch find contacts: pass all checked accounts as a multi-domain filter
  const batchFindContacts = () => {
    if (checkedAccounts.size === 0) return;
    const checkedCompanies = screenedCompanies.filter(c => checkedAccounts.has(c.domain));
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
      const people = (data.profiles || []).map(p => {
        const currentJob = (p.current_employers || [])[0] || {};
        return {
          id: p.person_id,
          name: p.name || '',
          headline: p.headline || '',
          title: currentJob.title || '',
          company: currentJob.name || '',
          companyDomain: currentJob.company_website_domain || '',
          seniority: currentJob.seniority_level || '',
          function: currentJob.function_category || '',
          linkedin: p.linkedin_profile_url || '',
          region: p.region || '',
          experience: p.years_of_experience_raw || 0,
          emailVerified: currentJob.business_email_verified || false,
          recentJobChange: p.recently_changed_jobs || false,
        };
      });
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

      // Close modal after adding
      setContactsModal(false);
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
    if (!campaignId) { setCampaignContacts([]); setCampaignMessages([]); return; }
    try {
      const [cts, msgs] = await Promise.all([
        getCampaignContacts(campaignId),
        getCampaignMessages(campaignId),
      ]);
      setCampaignContacts(cts);
      setCampaignMessages(msgs);
    } catch (err) {
      addLog(`Campaign load error: ${err.message}`);
    }
  };

  useEffect(() => {
    if (activeCampaignId) loadCampaignData(activeCampaignId);
  }, [activeCampaignId]);

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
      ? screenedCompanies.filter(c => checkedAccounts.has(c.domain))
      : screenedCompanies;
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

  const screenedCompanies = companies.filter(c => c.status === 'complete');
  const pendingCompanies = companies.filter(c => c.status === 'pending' || c.status === 'error' || c.status === 'processing');

  return (
    <div className="min-h-screen bg-gray-100">
      {/* Top navigation bar */}
      <div className="bg-white border-b border-gray-200 px-6 py-3">
        <div className="max-w-7xl mx-auto flex items-center">
          {/* Logo + title */}
          <div className="flex items-center gap-3 mr-8">
            <div className="w-9 h-9 rounded-xl bg-sky-500 flex items-center justify-center overflow-hidden">
              <img src="/icon.png" alt="Strata" className="w-7 h-7 object-contain" />
            </div>
            <div>
              <div className="text-sm font-bold text-gray-900 leading-tight">Strata ICP Screener</div>
              <div className="text-[10px] text-gray-400 leading-tight">Narrative gap analysis for B2B SaaS</div>
            </div>
          </div>
          {/* Nav tabs — gray pill bar */}
          <nav className="flex items-center bg-gray-100 rounded-lg p-1 gap-0.5">
            {[
              { key: 'discover', label: 'Discover' },
              { key: 'upload', label: 'Screen', badge: pendingCompanies.length > 0 ? pendingCompanies.length : null },
              { key: 'accounts', label: 'Accounts' },
              { key: 'contacts', label: 'Contacts' },
              { key: 'campaigns', label: 'Campaigns' },
            ].map(tab => (
              <button key={tab.key} onClick={() => setActiveView(tab.key)}
                className={`px-4 py-1.5 text-[13px] font-medium rounded-md transition-all ${
                  activeView === tab.key
                    ? 'bg-white text-gray-900 shadow-sm border border-gray-200'
                    : 'text-gray-500 hover:text-gray-700'
                }`}>
                {tab.label}
                {tab.badge && <span className="ml-1.5 px-1.5 py-0.5 bg-amber-100 text-amber-700 rounded-full text-[10px] font-bold">{tab.badge}</span>}
              </button>
            ))}
          </nav>
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
              <span>{screenedCompanies.length} accounts</span>
              {strongCount > 0 && <span className="text-green-600">{strongCount} strong</span>}
              {moderateCount > 0 && <span className="text-amber-600">{moderateCount} moderate</span>}
              {screenedCompanies.filter(c => (c.accountStatus || 'Cold') !== 'Cold').length > 0 && (
                <>
                  {ACCOUNT_STATUSES.filter(s => s !== 'Cold').map(s => {
                    const cnt = screenedCompanies.filter(c => c.accountStatus === s).length;
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
                let filtered = screenedCompanies;
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
                                  <span className={`inline-block w-7 h-7 rounded-full leading-7 text-center font-bold text-xs flex-shrink-0 ${c.totalScore >= 14 ? 'bg-green-100 text-green-600' : c.totalScore >= 10 ? 'bg-amber-100 text-amber-600' : 'bg-red-100 text-red-600'}`}>{c.totalScore}</span>
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
                          <span className={`font-medium ${c.totalScore >= 14 ? 'text-green-600' : c.totalScore >= 10 ? 'text-amber-600' : 'text-red-600'}`}>{c.totalScore}/18</span>
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
                        <button onClick={() => setSelectedCompany(null)} className="text-gray-500 hover:text-gray-900 text-lg">×</button>
                      </div>
                    </div>
                  </div>
                  
                  {c.scoreSummary && <div className="px-5 py-3 text-xs text-gray-500 leading-relaxed border-b border-gray-100">{c.scoreSummary}</div>}
                  
                  {/* Manual score */}
                  <div className="px-5 py-2 flex items-center gap-2 border-b border-gray-100">
                    <span className="text-[10px] text-gray-500">Manual Score:</span>
                    <input type="text" value={c.manualScore || ''} onChange={(e) => {
                      const val = e.target.value;
                      setCompanies(prev => { const u = [...prev]; u[origIdx] = { ...u[origIdx], manualScore: val }; return u; });
                      if (supabase && c.dbCompanyId) {
                        clearTimeout(window._manualScoreTimeout);
                        window._manualScoreTimeout = setTimeout(() => { dbUpdateCompany(c.dbCompanyId, { manual_score: val }).catch(() => {}); }, 1000);
                      }
                    }} placeholder="-" className="w-20 bg-gray-100 border border-gray-200 rounded px-2 py-1 text-xs text-center text-gray-700 focus:border-violet-500/50 focus:outline-none" />
                  </div>
                  
                  {/* Scoring breakdown — full detail panels */}
                  {c.scoringResult && (
                    <div className="px-5 py-4 border-b border-gray-100">
                      <div className="flex items-center justify-between mb-3">
                        <div className="text-[9px] text-gray-500 uppercase tracking-wider font-semibold">Scoring Breakdown</div>
                        <span className={`font-bold text-sm ${c.totalScore >= 14 ? 'text-green-600' : c.totalScore >= 10 ? 'text-amber-600' : 'text-red-600'}`}>{c.totalScore}/18</span>
                      </div>
                      <div className="space-y-4">
                        {[
                          { key: 'A', label: 'A. Differentiation', score: c.scoreA, just: c.scoreAJust, color: 'text-purple-400', borderColor: 'border-purple-500/20' },
                          { key: 'B', label: 'B. Outcomes', score: c.scoreB, just: c.scoreBJust, color: 'text-rose-600', borderColor: 'border-rose-500/20' },
                          { key: 'C', label: 'C. Customer-centric', score: c.scoreC, just: c.scoreCJust, color: 'text-orange-400', borderColor: 'border-orange-500/20' },
                          { key: 'D', label: 'D. Product change', score: c.scoreD, just: c.scoreDJust, color: 'text-cyan-400', borderColor: 'border-cyan-500/20' },
                          { key: 'E', label: 'E. Audience change', score: c.scoreE, just: c.scoreEJust, color: 'text-sky-600', borderColor: 'border-sky-500/20' },
                          { key: 'F', label: 'F. Multi-product', score: c.scoreF, just: c.scoreFJust, color: 'text-violet-600', borderColor: 'border-violet-500/20' },
                        ].map(({ key, label, score, just, color, borderColor }) => (
                          <div key={key}>
                            <div className="flex items-center gap-2 mb-1.5">
                              <span className={`${color} font-semibold text-xs`}>{label}</span>
                              <span className="flex gap-0.5">
                                {[1,2,3].map(n => (
                                  <span key={n} className={`inline-block w-3.5 h-1.5 rounded-full ${n <= score ? (score === 3 ? 'bg-green-400' : score === 2 ? 'bg-amber-400' : 'bg-red-400') : 'bg-gray-200'}`} />
                                ))}
                              </span>
                              <span className="text-gray-500 text-[10px]">+{score}</span>
                            </div>
                            <div className={`p-2.5 bg-gray-50/60 rounded-md border ${borderColor}`}>
                              <FactorPanel factorKey={key} data={just} />
                            </div>
                          </div>
                        ))}
                      </div>
                      {c.scoreSummary && (
                        <div className="mt-3 pt-3 border-t border-gray-100 text-[11px]">
                          <span className="text-gray-500 font-semibold">Summary: </span>
                          <span className="text-gray-400">{c.scoreSummary}</span>
                        </div>
                      )}
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
                  
                  {/* Marketing leader + PMM grid */}
                  <div className="px-5 py-3 border-b border-gray-100">
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <div className="text-[9px] text-gray-500 uppercase tracking-wider font-semibold mb-1">New Marketing Leader</div>
                        <div className="text-[11px] text-gray-500">{c.newMarketingLeader || 'None found'}</div>
                      </div>
                      <div>
                        <div className="text-[9px] text-gray-500 uppercase tracking-wider font-semibold mb-1">Product Marketing Team</div>
                        <div className="text-[11px] text-gray-500">{c.productMarketingPeople || 'None found'}</div>
                      </div>
                    </div>
                  </div>
                  
                  {/* Research collapsibles */}
                  <div className="px-5 py-3 border-b border-gray-100 flex flex-col gap-0.5">
                    <Collapsible title="Research: Company Overview">
                      <div className="flex flex-col gap-1">
                        {c.productSummary && <div><span className="text-gray-500">Product:</span> {c.productSummary}</div>}
                        {c.targetCustomer && <div><span className="text-gray-500">Buyer:</span> {c.targetCustomer}</div>}
                        {c.funding && <div><span className="text-gray-500">Funding:</span> {c.funding}</div>}
                        {c.teamSize && <div><span className="text-gray-500">Team:</span> {c.teamSize}{c.customers ? <>{' · '}<span className="text-gray-500">Customers:</span> {c.customers}</> : ''}</div>}
                      </div>
                    </Collapsible>
                    {c.ceoFounderName && (
                      <Collapsible title="Research: CEO/Founder Voice">
                        <div className="flex flex-col gap-1">
                          <div><span className="text-gray-500">CEO:</span> {c.ceoFounderName}</div>
                          {c.ceoNarrativeTheme && <div className="text-amber-300">{c.ceoNarrativeTheme}</div>}
                          {c.ceoRecentContent && <div><span className="text-gray-500">Recent:</span> {c.ceoRecentContent}</div>}
                        </div>
                      </Collapsible>
                    )}
                    <Collapsible title="Raw: Research Output">
                      <pre className="text-[10px] text-gray-500 leading-relaxed whitespace-pre-wrap max-h-64 overflow-auto">{c.researchResult}</pre>
                    </Collapsible>
                    <Collapsible title="Raw: Scoring Output">
                      <pre className="text-[10px] text-gray-500 leading-relaxed whitespace-pre-wrap max-h-64 overflow-auto">{c.scoringResult}</pre>
                    </Collapsible>
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
                  {screenedCompanies.length > 0 && (
                    <div className="relative">
                      <button onClick={() => setFindContactsPicker(findContactsPicker === 'contacts' ? null : 'contacts')} className="px-3 py-1.5 bg-violet-600 hover:bg-violet-500 text-white rounded-lg text-xs font-medium">Find More Contacts ▾</button>
                      {findContactsPicker === 'contacts' && (
                        <div className="absolute right-0 top-8 z-50 bg-white border border-gray-200 rounded-lg shadow-xl py-1 min-w-[220px] max-h-[300px] overflow-auto">
                          <div className="px-3 py-1.5 text-[10px] text-gray-400 uppercase tracking-wide">Select account</div>
                          {screenedCompanies.map(c => (
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
              const linkedAccount = screenedCompanies.find(c => c.domain === ct.company_domain);
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
                          <span className={`inline-block w-8 h-8 rounded-full leading-8 text-center font-bold text-sm ${linkedAccount.totalScore >= 14 ? 'bg-green-100 text-green-600' : linkedAccount.totalScore >= 10 ? 'bg-amber-100 text-amber-600' : 'bg-red-100 text-red-600'}`}>{linkedAccount.totalScore}</span>
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
        {activeView === 'discover' && (
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
                  <span className="text-xs text-gray-500 ml-auto">{discoverResults.length} results · {discoverTotal.toLocaleString()} total matches</span>
                </div>
                <div className="bg-white border border-gray-200 rounded-lg overflow-hidden">
                  <div className="grid grid-cols-12 gap-2 px-4 py-2 text-[10px] text-gray-400 uppercase tracking-wide font-medium border-b border-gray-100">
                    <div className="col-span-1"></div>
                    <div className="col-span-3">Company</div>
                    <div className="col-span-3">Domain</div>
                    <div className="col-span-3">Industry</div>
                    <div className="col-span-2 text-right">Employees</div>
                  </div>
                  {discoverResults.map((co) => (
                    <div key={co.domain} onClick={() => toggleDiscoverSelect(co.domain)}
                      className={`grid grid-cols-12 gap-2 px-4 py-2.5 items-center cursor-pointer transition-colors border-b border-gray-100 last:border-0 ${
                        discoverSelected.has(co.domain) ? 'bg-violet-50' : 'hover:bg-white/3'
                      }`}>
                      <div className="col-span-1">
                        <div className={`w-4 h-4 rounded border flex items-center justify-center text-[10px] ${
                          discoverSelected.has(co.domain) ? 'bg-violet-500 border-violet-500 text-white' : 'border-gray-600'
                        }`}>{discoverSelected.has(co.domain) && '✓'}</div>
                      </div>
                      <div className="col-span-3 text-sm text-gray-900 font-medium truncate">{co.name}</div>
                      <div className="col-span-3 text-xs text-gray-500 truncate">{co.domain}</div>
                      <div className="col-span-3 text-xs text-gray-500 truncate">{co.industry}</div>
                      <div className="col-span-2 text-sm text-gray-500 text-right">{co.employees ? co.employees.toLocaleString() : '-'}</div>
                    </div>
                  ))}
                </div>
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
              <div className="bg-white border border-gray-200 rounded-xl overflow-hidden" style={{ minHeight: '75vh' }}>
                <div className="flex divide-x divide-gray-200 h-full" style={{ minHeight: '75vh' }}>
                {/* LEFT: Campaign contacts list */}
                <div className="w-[240px] min-w-[200px] flex flex-col">
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
                            <button onClick={(e) => { e.stopPropagation(); }}
                              className="text-gray-300 hover:text-red-400 text-[11px] ml-1">×</button>
                          </div>
                        </div>
                      );
                    })}
                    {campaignContacts.length === 0 && (
                      <div className="text-center py-10 text-gray-400 text-sm">No contacts yet. Select contacts in the Contacts tab and use "+ Campaign" to add them.</div>
                    )}
                  </div>
                </div>

                {/* MIDDLE: Selected contact detail */}
                <div className="w-[280px] min-w-[240px] flex flex-col">
                  {selectedCampaignContact ? (() => {
                    const cc = campaignContacts.find(c => c.contact_id === selectedCampaignContact);
                    const ct = cc?.contacts;
                    if (!ct) return <div className="text-gray-400 text-[13px]">Contact not found</div>;
                    const companyBasic = ct.companies;
                    // Look up full account data from in-memory companies array
                    const fullAccount = companyBasic?.domain ? companies.find(c => c.domain === companyBasic.domain && c.status === 'complete') : null;
                    return (
                      <div className="overflow-auto h-full">
                        <div className="px-5 py-4 border-b border-gray-100">
                          <div className="flex items-center gap-3">
                            <span className="w-10 h-10 rounded-full bg-violet-100 flex items-center justify-center text-base font-bold text-violet-600">
                              {(ct.name || '?')[0].toUpperCase()}
                            </span>
                            <div>
                              <h3 className="text-[15px] font-bold text-gray-900">{ct.name}</h3>
                              <div className="text-[11px] text-gray-500">{ct.title}</div>
                            </div>
                          </div>
                        </div>
                        <div className="px-5 py-3 space-y-2.5">
                          {/* Company */}
                          {companyBasic && (
                            <div>
                              <div className="text-[10px] text-gray-400 uppercase tracking-wide mb-0.5">Company</div>
                              <div className="text-[13px] font-semibold text-gray-900">{companyBasic.name}</div>
                            </div>
                          )}
                          {/* Email */}
                          <div>
                            <div className="text-[10px] text-gray-400 uppercase tracking-wide mb-0.5">Email</div>
                            {ct.business_email ? (
                              <div className="flex items-center gap-1.5">
                                <a href={`mailto:${ct.business_email}`} className="text-xs text-violet-600 hover:text-violet-700">{ct.business_email}</a>
                                <button onClick={() => navigator.clipboard.writeText(ct.business_email)} className="text-[9px] text-gray-400 hover:text-gray-600 px-1.5 py-0.5 bg-gray-100 rounded">Copy</button>
                              </div>
                            ) : (
                              <div className="flex items-center gap-2">
                                <span className="text-xs text-amber-600">⚠ No email</span>
                                {ct.linkedin && (
                                  <button onClick={() => enrichContact(ct)} disabled={enrichingContacts[ct.id] === 'loading'}
                                    className="px-2 py-1 bg-violet-50 hover:bg-violet-100 text-violet-600 rounded text-[10px] font-medium border border-violet-200">
                                    {enrichingContacts[ct.id] === 'loading' ? 'Finding...' : 'Find Email'}
                                  </button>
                                )}
                              </div>
                            )}
                          </div>
                          {/* LinkedIn */}
                          {ct.linkedin && (
                            <div>
                              <div className="text-[10px] text-gray-400 uppercase tracking-wide mb-0.5">LinkedIn</div>
                              <a href={ct.linkedin} target="_blank" rel="noopener noreferrer" className="text-xs text-sky-600 hover:text-sky-700 truncate block">
                                {ct.linkedin.replace('https://www.linkedin.com/in/', '').replace('https://linkedin.com/in/', '').replace(/\/$/, '')}
                              </a>
                            </div>
                          )}
                          {/* Fit assessment from account */}
                          {fullAccount && fullAccount.icpFit && (
                            <div className={`mt-2 p-3 rounded-lg border text-xs ${
                              fullAccount.icpFit === 'Strong' ? 'bg-green-50 border-green-200' :
                              fullAccount.icpFit === 'Moderate' ? 'bg-amber-50 border-amber-200' :
                              'bg-red-50 border-red-200'
                            }`}>
                              <div className={`font-semibold mb-1 ${
                                fullAccount.icpFit === 'Strong' ? 'text-green-700' :
                                fullAccount.icpFit === 'Moderate' ? 'text-amber-700' :
                                'text-red-700'
                              }`}>✓ {fullAccount.icpFit} Fit</div>
                              {fullAccount.fitExplanation && (
                                <div className="text-gray-600 leading-relaxed">{fullAccount.fitExplanation.substring(0, 200)}{fullAccount.fitExplanation.length > 200 ? '...' : ''}</div>
                              )}
                            </div>
                          )}
                          {/* Account details dropdown */}
                          {fullAccount && (
                            <details className="mt-2">
                              <summary className="text-[10px] text-gray-400 uppercase tracking-wide cursor-pointer hover:text-gray-600 select-none">
                                Account Details
                              </summary>
                              <div className="mt-2 p-3 bg-gray-50 rounded-lg border border-gray-100 space-y-2 text-[11px]">
                                <div className="grid grid-cols-2 gap-x-4 gap-y-1.5">
                                  <div><span className="text-gray-400">Score:</span> <span className="font-semibold text-gray-700">{fullAccount.totalScore}/18</span></div>
                                  <div><span className="text-gray-400">ICP Fit:</span> <span className="text-gray-700">{fullAccount.icpFit || 'N/A'}</span></div>
                                  <div><span className="text-gray-400">Industry:</span> <span className="text-gray-700">{fullAccount.industry || 'N/A'}</span></div>
                                  <div><span className="text-gray-400">Employees:</span> <span className="text-gray-700">{fullAccount.employees ? fullAccount.employees.toLocaleString() : 'N/A'}</span></div>
                                  <div><span className="text-gray-400">Funding:</span> <span className="text-gray-700">{fullAccount.funding || 'N/A'}</span></div>
                                  <div><span className="text-gray-400">Domain:</span> <a href={`https://${fullAccount.domain}`} target="_blank" rel="noopener noreferrer" className="text-sky-600 hover:text-sky-700">{fullAccount.domain}</a></div>
                                </div>
                                <div className="border-t border-gray-200 pt-2 mt-2">
                                  <div className="text-[10px] text-gray-400 uppercase mb-1">Factor Scores</div>
                                  <div className="grid grid-cols-3 gap-1.5">
                                    {[
                                      { label: 'Narrative Gap', score: fullAccount.scoreA },
                                      { label: 'Timing', score: fullAccount.scoreB },
                                      { label: 'Complexity', score: fullAccount.scoreC },
                                      { label: 'Differentiation', score: fullAccount.scoreD },
                                      { label: 'Exec Alignment', score: fullAccount.scoreE },
                                      { label: 'Content Gap', score: fullAccount.scoreF },
                                    ].map(f => (
                                      <div key={f.label} className="flex items-center gap-1">
                                        <span className={`w-5 h-5 rounded flex items-center justify-center text-[10px] font-bold ${
                                          f.score >= 3 ? 'bg-green-100 text-green-700' : f.score >= 2 ? 'bg-amber-100 text-amber-700' : 'bg-red-100 text-red-700'
                                        }`}>{f.score}</span>
                                        <span className="text-gray-500 text-[10px] truncate">{f.label}</span>
                                      </div>
                                    ))}
                                  </div>
                                </div>
                                {fullAccount.summary && (
                                  <div className="border-t border-gray-200 pt-2 mt-2">
                                    <div className="text-[10px] text-gray-400 uppercase mb-1">Summary</div>
                                    <div className="text-gray-600 leading-relaxed">{fullAccount.summary.substring(0, 300)}{fullAccount.summary.length > 300 ? '...' : ''}</div>
                                  </div>
                                )}
                              </div>
                            </details>
                          )}
                        </div>
                      </div>
                    );
                  })() : (
                    <div className="flex flex-col items-center justify-center h-full text-gray-400">
                      <div className="text-4xl mb-3">👤</div>
                      <div className="text-sm">Select a contact</div>
                    </div>
                  )}
                </div>

                {/* RIGHT: Messages */}
                <div className="flex-1 flex flex-col">
                  <div className="flex items-center justify-between px-5 py-3 border-b border-gray-100">
                    <div className="text-[11px] text-gray-400 uppercase tracking-wide font-medium">Messages ({campaignMessages.length})</div>
                    <div className="flex gap-2">
                      <button onClick={() => handleAddMessage('email')} className="px-3 py-1 bg-violet-50 hover:bg-violet-100 text-violet-600 border border-violet-200 rounded-lg text-[11px] font-medium">+ Email</button>
                      <button onClick={() => handleAddMessage('linkedin')} className="px-3 py-1 bg-gray-50 hover:bg-gray-100 text-gray-600 border border-gray-200 rounded-lg text-[11px] font-medium">+ LinkedIn</button>
                    </div>
                  </div>
                  <div className="flex-1 overflow-auto p-4 space-y-4">
                    {campaignMessages.map(msg => (
                      <CampaignMessageCard key={msg.id} msg={msg} onSave={handleSaveMessage} onDelete={handleDeleteMessage} />
                    ))}
                    {campaignMessages.length === 0 && (
                      <div className="text-center py-16 text-gray-400 text-[13px]">No messages yet. Click "+ Email" or "+ LinkedIn" to add a step.</div>
                    )}
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
        {contactsModal && (
          <div className="fixed inset-0 bg-black/40 flex items-center justify-center z-50 p-4">
            <div className="bg-white border border-gray-200 rounded-lg w-full max-w-5xl max-h-[90vh] flex flex-col">
              {/* Header */}
              <div className="flex items-center justify-between px-6 py-4 border-b border-gray-200">
                <div>
                  <h3 className="text-lg font-bold text-gray-900">
                    {Array.isArray(contactsCompany)
                      ? `Contacts across ${contactsCompany.length} accounts`
                      : contactsCompany ? `Contacts at ${contactsCompany.name}` : 'Select an account'}
                  </h3>
                  <p className="text-xs text-gray-500 mt-0.5 max-w-[600px] truncate">
                    {Array.isArray(contactsCompany)
                      ? contactsCompany.map(c => c.name).join(', ')
                      : contactsCompany ? contactsCompany.domain : 'No account selected'}
                    {contactsTotal > 0 && ` · ${contactsTotal.toLocaleString()} total matches`}
                  </p>
                </div>
                <button onClick={() => setContactsModal(false)} className="text-gray-500 hover:text-gray-900 text-xl leading-none">×</button>
              </div>
              
              {/* Filters */}
              <div className="px-6 py-4 border-b border-gray-100 space-y-3">
                {/* Function — primary filter */}
                <div>
                  <label className="block text-xs text-gray-500 mb-1.5">Function</label>
                  <div className="flex flex-wrap gap-1.5">
                    {FUNCTION_OPTIONS.map(fn => (
                      <button key={fn} onClick={() => setContactsFilters(f => ({
                        ...f, functions: f.functions.includes(fn) ? f.functions.filter(x => x !== fn) : [...f.functions, fn]
                      }))}
                        className={`px-2.5 py-1 rounded text-xs transition-colors ${
                          contactsFilters.functions.includes(fn)
                            ? 'bg-violet-100 text-violet-700 border border-violet-300'
                            : 'bg-gray-100 text-gray-500 border border-gray-200 hover:text-gray-700'
                        }`}
                      >{fn}</button>
                    ))}
                  </div>
                </div>
                {/* Job Title — autocomplete search */}
                <div className="relative">
                  <label className="block text-xs text-gray-500 mb-1">Job Title (optional)</label>
                  <input
                    type="text" placeholder="Type to search titles, e.g. CMO, VP Marketing..."
                    value={contactsFilters.titles}
                    onChange={e => {
                      const val = e.target.value;
                      setContactsFilters(f => ({ ...f, titles: val }));
                      // Autocomplete on the last comma-separated segment
                      const segments = val.split(',');
                      const lastSegment = segments[segments.length - 1].trim();
                      fetchTitleSuggestions(lastSegment);
                    }}
                    onFocus={() => {
                      const segments = contactsFilters.titles.split(',');
                      const lastSegment = segments[segments.length - 1].trim();
                      if (lastSegment.length >= 2) fetchTitleSuggestions(lastSegment);
                    }}
                    onBlur={() => setTimeout(() => setTitleSuggestions([]), 200)}
                    className="w-full bg-gray-100 border border-gray-200 rounded px-3 py-1.5 text-sm text-gray-700 placeholder-gray-400"
                  />
                  {titleSuggestions.length > 0 && (
                    <div className="absolute left-0 right-0 top-full mt-1 bg-white border border-gray-200 rounded-lg shadow-xl z-50 max-h-[200px] overflow-auto">
                      {titleSuggestions.map((s, i) => (
                        <button key={i} onMouseDown={(e) => {
                          e.preventDefault();
                          // Replace last segment with selected suggestion
                          const segments = contactsFilters.titles.split(',').map(t => t.trim()).filter(Boolean);
                          segments[segments.length > 0 ? segments.length - 1 : 0] = s;
                          setContactsFilters(f => ({ ...f, titles: segments.join(', ') }));
                          setTitleSuggestions([]);
                        }}
                          className="block w-full text-left px-4 py-2 text-sm text-gray-700 hover:bg-violet-50 hover:text-violet-700 truncate">
                          {s}
                        </button>
                      ))}
                    </div>
                  )}
                  {titleAutocompleteLoading && (
                    <div className="absolute right-3 top-7 text-[10px] text-gray-400">searching...</div>
                  )}
                </div>
                {/* Options row */}
                <div className="flex items-center gap-4">
                  <label className="flex items-center gap-1.5 cursor-pointer">
                    <input type="checkbox" checked={contactsFilters.verifiedEmailOnly}
                      onChange={e => setContactsFilters(f => ({ ...f, verifiedEmailOnly: e.target.checked }))}
                      className="rounded bg-gray-100 border-gray-600 accent-violet-500" />
                    <span className="text-xs text-gray-500">Verified email only</span>
                  </label>
                  <label className="flex items-center gap-1.5 cursor-pointer">
                    <input type="checkbox" checked={contactsFilters.recentlyChangedJobs}
                      onChange={e => setContactsFilters(f => ({ ...f, recentlyChangedJobs: e.target.checked }))}
                      className="rounded bg-gray-100 border-gray-600 accent-violet-500" />
                    <span className="text-xs text-gray-500">Recently changed jobs</span>
                  </label>
                </div>
                <div className="flex items-center gap-3">
                  <button onClick={() => searchContacts(false)} disabled={contactsLoading}
                    className="px-5 py-2 bg-violet-600 hover:bg-violet-500 disabled:bg-violet-600/50 text-white rounded-lg text-sm font-medium">
                    {contactsLoading ? 'Searching...' : 'Search'}
                  </button>
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
                    <div className="grid grid-cols-12 gap-2 px-3 py-1.5 text-[10px] text-gray-400 uppercase tracking-wide font-medium sticky top-0 bg-white/95 backdrop-blur z-10">
                      <div className="col-span-1 flex items-center">
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
                      </div>
                      <div className="col-span-3">Name</div>
                      <div className="col-span-2">Title</div>
                      <div className="col-span-2">Company</div>
                      <div className="col-span-1">Seniority</div>
                      <div className="col-span-1">Function</div>
                      <div className="col-span-1">Region</div>
                      <div className="col-span-1 text-right">Links</div>
                    </div>
                    {contactsResults.map((p) => {
                      const rowKey = p.id || p.linkedin;
                      const isChecked = selectedContactResults.has(rowKey);
                      return (
                      <div key={rowKey}
                        onClick={() => setSelectedContactResults(prev => {
                          const next = new Set(prev);
                          if (next.has(rowKey)) next.delete(rowKey); else next.add(rowKey);
                          return next;
                        })}
                        className={`grid grid-cols-12 gap-2 px-3 py-2 rounded items-center text-sm cursor-pointer transition-colors ${
                          isChecked ? 'bg-violet-50 hover:bg-violet-50' : 'hover:bg-white/3'
                        }`}>
                        <div className="col-span-1 flex items-center">
                          <input type="checkbox" checked={isChecked} readOnly
                            className="rounded bg-gray-100 border-gray-600 text-violet-500 w-3.5 h-3.5 pointer-events-none" />
                        </div>
                        <div className="col-span-3">
                          <div className="text-gray-900 text-sm font-medium truncate">{p.name}</div>
                          {p.emailVerified && <span className="text-[10px] text-emerald-600">✓ email</span>}
                          {p.recentJobChange && <span className="text-[10px] text-amber-600 ml-1">★ new role</span>}
                        </div>
                        <div className="col-span-2 text-gray-500 text-xs truncate" title={p.title}>{p.title}</div>
                        <div className="col-span-2 text-gray-500 text-xs truncate">{p.company}</div>
                        <div className="col-span-1 text-gray-500 text-xs">{p.seniority}</div>
                        <div className="col-span-1 text-gray-500 text-xs truncate">{p.function}</div>
                        <div className="col-span-1 text-gray-400 text-xs truncate">{p.region}</div>
                        <div className="col-span-1 text-right">
                          {p.linkedin && (
                            <a href={p.linkedin} target="_blank" rel="noopener noreferrer"
                              onClick={e => e.stopPropagation()}
                              className="text-violet-600 hover:text-violet-700 text-xs">LI</a>
                          )}
                        </div>
                      </div>
                      );
                    })}
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
          </div>
        )}
      </div>
    </div>
  );
}
