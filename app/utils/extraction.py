
import google.generativeai as genai
import pdfplumber
import pandas as pd
import nltk
import re
import json
import os
import logging
from nltk.tokenize import sent_tokenize

# Download NLTK data (this will happen automatically on first run)
try:
    nltk.download('punkt', quiet=True)
    nltk.download('punkt_tab', quiet=True)
except:
    pass

def setup_gemini(api_key):
    genai.configure(api_key=api_key)
    return genai.GenerativeModel('gemini-2.0-flash')

def extract_text_from_pdf(pdf_path):
    logging.info(f"Extracting text from PDF: {pdf_path}")
    full_text = ""
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                full_text += text + "\n"
    logging.info(f"Extracted {len(full_text)} characters of text from PDF.")
    return full_text

def extract_pagewise_context(pdf_path, keywords):
    logging.info(f"Extracting pagewise context with keywords: {keywords}")
    contexts = []
    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            text = page.extract_text()
            if not text:
                continue
            # Clean linebreaks
            text = re.sub(r'-\s*\n', '', text)
            text = re.sub(r'\s*\n\s*', ' ', text)

            sentences = sent_tokenize(text)
            for i, sentence in enumerate(sentences):
                if any(k.lower() in sentence.lower() for k in keywords):
                    start = max(i - 2, 0)
                    end = min(i + 3, len(sentences))
                    context = " ".join(sentences[start:end])
                    contexts.append({"page": page_num, "context": context})
                    logging.info(f"[Page {page_num}] {context}")
    logging.info(f"✅ Found {len(contexts)} pagewise contexts with keywords: {keywords}")
    return contexts

def prepare_prompt(batch_contexts):
    combined_text = "\n\n".join(
        [f"(Page {c['page']}) {c['context']}" for c in batch_contexts]
    )
    prompt = f"""
Role: You are a specialized financial FINE extraction system for RBI documents.
You must ONLY extract details about FINES directly from the given RBI PDF text.
⚠️ Do NOT add, assume, or invent any extra laws, fines, or explanations that are not explicitly written in the PDF text.

Task: From the provided RBI PDF snippets, identify and extract ONLY FINE-related information. Return a JSON array.
Each JSON object should strictly use text found in the PDF.

For each FINE you find, include ONLY these fields:

- id: serial number (from SL No column)
- Circular / Direction: the circular, direction, or section text as written
- Violation Type: violation or non-compliance description as written
- penalty_amount_text: the exact FINE wording from the PDF (if any)
- normalized_amount: {{ "lower": number, "upper": number }} if numeric values are found, else empty object {{}}
- currency: currency symbol/code if explicitly written (₹ / INR / Rs. etc.)
- Legal Provision Invoked: exact law, section, or guideline mentioned in the PDF
- reason_text: the reason or clause text directly from the PDF
- Page: page number from the context snippet

⚠️ CRITICAL RULES:
1. ONLY extract information that contains the word "FINE" or "FINES"
2. Ignore all other content that doesn't contain explicit FINE information
3. Only use words, numbers, and references explicitly present in the PDF
4. If fine amount is missing, leave the field empty
5. If law or provision is not explicitly written, leave the field empty
6. If no FINE is mentioned, DO NOT include it in the output
7. Output ONLY valid JSON array, nothing else
8. DO NOT confuse years (like 2012, 2005, 2009, 2010, 2014) with fine amounts
9. Only extract amounts that are clearly FINES (must have the word "fine" nearby)
10. If uncertain whether something is a fine, leave it out

💰 STRICT FINE EXTRACTION:
- ONLY extract if you see the exact word: "fine" or "fines"
- Look for patterns: "fine of ₹X", "fines up to ₹Y", "liable to fine"
- Extract the exact legal provision text as written in the PDF
- Penalties

🚫 IGNORE THESE (NOT FINES):

- Levies, impositions, sanctions without the word "fine"
- Annual fees, charges, costs
- Transaction amounts, loan amounts
- Years, dates, page numbers, section numbers
- Administrative fees, service charges
- Any amount not explicitly described as a FINE

📌 Example Output Format (ONLY FOR FINES):

```json
[
  {{
    "id": "1",
    "Circular / Direction": "Section 13 of PMLA Act",
    "Violation Type": "Failure to comply with reporting obligations",
    "penalty_amount_text": "fine which may extend to one lakh rupees",
    "normalized_amount": {{ "lower": 1000, "upper": 100000 }},
    "currency": "₹",
    "Legal Provision Invoked": "Section 13(2) of Prevention of Money Laundering Act, 2002",
    "reason_text": "failed to comply with the obligations under this Chapter",
    "Page": "21"
  }}
]

🚫 BAD EXAMPLES (what to avoid):
- DO NOT extract content without the word "fine"
- DO NOT extract "2012" as an amount (this is a year)
- DO NOT extract "2005" as an amount (this is a year)  
- DO NOT extract transaction amounts that are not fines
- DO NOT extract page numbers as amounts
- DO NOT extract section numbers as amounts
- DO NOT invent fines that aren't explicitly mentioned

✅ ONLY EXTRACT IF YOU SEE THIS EXACT WORD: "fine" or "fines"

Now analyze this text and extract ONLY FINE information:

{combined_text}
"""
    return prompt

def process_in_batches(contexts, model, batch_size=2):
    all_penalties = []
    total_batches = (len(contexts) - 1) // batch_size + 1
    logging.info(f"Starting batch processing of {len(contexts)} contexts...")
    for i in range(0, len(contexts), batch_size):
        batch_num = i // batch_size + 1
        batch = contexts[i:i+batch_size]
        logging.info(f"🔹 Processing batch {batch_num}/{total_batches} ({len(batch)} contexts)")
        if len(all_penalties) > 20:
            logging.warning("⚠️ Stopping early due to API limitations")
            break
        prompt = prepare_prompt(batch)
        try:
            response = model.generate_content(prompt)
            batch_penalties = parse_ai_response(response.text)
            if batch_penalties:
                logging.info(f"✅ Extracted {len(batch_penalties)} fines from this batch")
                all_penalties.extend(batch_penalties)
            else:
                logging.info("⚠️ No fines found in this batch")
        except Exception as e:
            error_msg = str(e)
            logging.error(f"❌ Error processing batch: {error_msg}")
            if "quota" in error_msg.lower() or "429" in error_msg or "exceeded" in error_msg.lower():
                logging.error("🚫 API quota exceeded. Stopping processing.")
                break
            continue
        import time
        time.sleep(5)
    logging.info(f"✅ Completed batch processing. Total fines extracted: {len(all_penalties)}")
    return all_penalties

def parse_ai_response(response_text):
    try:
        # Extract JSON from response
        json_match = re.search(r'\[.*\]', response_text, re.DOTALL)
        if json_match:
            json_str = json_match.group(0)
            return json.loads(json_str)
        return []
    except:
        return []


# Restore process_rbi_pdf as a function
def process_rbi_pdf(pdf_path, api_key):
    logging.info("Setting up Gemini model for analysis.")
    model = setup_gemini(api_key)
    # Extract text
    raw_text = extract_text_from_pdf(pdf_path)
    # Get contexts
    keywords = ['fine',]
    pagewise_contexts = extract_pagewise_context(pdf_path, keywords)
    # Process in batches
    all_penalties = process_in_batches(pagewise_contexts, model, batch_size=2)
    # Convert to DataFrame and return
    logging.info(f"Processing {len(all_penalties)} extracted penalties...")
    if all_penalties:
        df = pd.DataFrame(all_penalties)
        logging.info(f"Extracted DataFrame:\n{df.to_string(index=False)}")
        return process_dataframe(df, os.path.basename(pdf_path))
    else:
        logging.info("⚠️ No penalty data found to display.")
        return process_dataframe(pd.DataFrame(), os.path.basename(pdf_path))

def process_dataframe(df, pdf_name):
    import re
    import numpy as np
    # Helper functions
    WORD_NUMS = {
        "zero":0,"one":1,"two":2,"three":3,"four":4,"five":5,"six":6,"seven":7,"eight":8,"nine":9,
        "ten":10,"eleven":11,"twelve":12,"thirteen":13,"fourteen":14,"fifteen":15,"sixteen":16,
        "seventeen":17,"eighteen":18,"nineteen":19,"twenty":20,"thirty":30,"forty":40,"fifty":50,
        "sixty":60,"seventy":70,"eighty":80,"ninety":90,"hundred":100
    }
    SCALE = {"thousand":1_000, "lakh":100_000, "lakhs":100_000, "crore":10_000_000, "crores":10_000_000}
    def words_to_number_phrase(text):
        nums = []
        pattern = re.compile(r"\\b(?:(?:one|two|three|four|five|six|seven|eight|nine|ten|"
                             r"eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|"
                             r"eighteen|nineteen|twenty|thirty|forty|fifty|sixty|seventy|"
                             r"eighty|ninety)(?:\\s+hundred)?)\\s+(thousand|lakh|lakhs|crore|crores)\\b",
                             flags=re.I)
        for m in pattern.finditer(text):
            phrase = m.group(0).lower()
            parts = phrase.split()
            base = 0
            i = 0
            while i < len(parts) and parts[i] not in SCALE:
                word = parts[i]
                if word == "hundred":
                    base *= 100
                else:
                    base += WORD_NUMS.get(word, 0)
                i += 1
            scale_word = parts[-1]
            total = base * SCALE.get(scale_word, 1)
            if total:
                nums.append(total)
        simple = re.compile(r"\\b(one|two|three|four|five|six|seven|eight|nine|ten|"
                            r"eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|"
                            r"eighteen|nineteen|twenty|thirty|forty|fifty|sixty|seventy|"
                            r"eighty|ninety)\\s+(thousand|lakh|lakhs|crore|crores)\\b", re.I)
        for m in simple.finditer(text):
            base = WORD_NUMS.get(m.group(1).lower(), 0)
            total = base * SCALE.get(m.group(2).lower(), 1)
            if total and total not in nums:
                nums.append(total)
        return nums
    def detect_currency(text):
        if re.search(r"₹|rupee|rupees|rs\\.?", text, flags=re.I):
            return "INR"
        return ""
    def extract_amounts(row):
        lb, ub = None, None
        text_for_display = ""
        if isinstance(row.get("normalized_amount"), dict):
            lb = row["normalized_amount"].get("lower")
            ub = row["normalized_amount"].get("upper")
        if lb is None and ub is None:
            penalty_text = "" if pd.isna(row.get("penalty_amount_text")) else str(row.get("penalty_amount_text"))
            reason_text = "" if pd.isna(row.get("reason_text")) else str(row.get("reason_text"))
            summary_text = "" if pd.isna(row.get("summary_sentence")) else str(row.get("summary_sentence"))
            combined_text = " ".join([penalty_text, reason_text, summary_text])
            text_for_display = combined_text
            nums = re.findall(r"\d[\d,]*", combined_text)
            numbers = []
            for n in nums:
                try:
                    num_val = int(n.replace(",", ""))
                    if not (1900 <= num_val <= 2099) and not (1 <= num_val <= 31):
                        numbers.append(num_val)
                except ValueError:
                    continue
            word_numbers = words_to_number_phrase(combined_text)
            numbers.extend(word_numbers)
            numbers = [n for n in numbers if n >= 1000]
            numbers = sorted(set(numbers))
            if len(numbers) == 1:
                lb = ub = numbers[0]
            elif len(numbers) >= 2:
                lb, ub = numbers[0], numbers[-1]
        def fmt(n): return f"₹{n:,}" if n is not None else ""
        if lb is not None and ub is not None and lb != ub:
            penalty_range = f"{fmt(lb)} – {fmt(ub)}"
        elif lb is not None:
            penalty_range = fmt(lb)
        else:
            penalty_range = text_for_display.strip()
        return lb, ub, penalty_range
    LEGAL_PATTERNS = [
        r"(Section\\s+[0-9A-Za-z()./-]+(?:\\s*\\([^)]+\\))?\\s+of\\s+[A-Za-z ]+?Act,\\s*\\d{4})",
        r"(Prevention of Money Laundering(?:\\s*\\(Amendment\\))?\\s*Act,\\s*\\d{4})",
        r"(Banking Regulation Act,\\s*1949)",
        r"(RBI Act,\\s*1934)"
    ]
    def extract_legal_from_row(row):
        legal_text = row.get("Legal Provision Invoked")
        if not pd.isna(legal_text) and str(legal_text).strip():
            return legal_text
        circular_text = "" if pd.isna(row.get("Circular / Direction")) else str(row.get("Circular / Direction"))
        reason_text = "" if pd.isna(row.get("reason_text")) else str(row.get("reason_text"))
        summary_text = "" if pd.isna(row.get("summary_sentence")) else str(row.get("summary_sentence"))
        penalty_text = "" if pd.isna(row.get("penalty_amount_text")) else str(row.get("penalty_amount_text"))
        blob = " ".join([circular_text, reason_text, summary_text, penalty_text])
        hits = []
        for pat in LEGAL_PATTERNS:
            for m in re.findall(pat, blob, flags=re.I):
                val = m if isinstance(m, str) else " ".join(m)
                hits.append(val.strip())
        seen, uniq = set(), []
        for h in hits:
            if h.lower() not in seen:
                seen.add(h.lower()); uniq.append(h)
        return "; ".join(uniq) if uniq else ""
    # Main logic
    if df.empty:
        final_cols = [
            "SL No", "Circular / Direction", "Violation Type", "Penalty Range",
            "Legal Provision Invoked", "Reason / Description", "Page"
        ]
        return pd.DataFrame(columns=final_cols)
    expected_cols = [
        "id", "Circular / Direction", "Violation Type", "penalty_amount_text",
        "Legal Provision Invoked", "reason_text", "Page", "normalized_amount",
        "summary_sentence", "currency"
    ]
    for col in expected_cols:
        if col not in df.columns:
            df[col] = ""
    df = df.replace("", pd.NA)
    def prefix_pdf_name(text):
        if pd.isna(text) or not str(text).strip():
            return f"({pdf_name})"
        elif not str(text).startswith(f"({pdf_name})"):
            return f"({pdf_name}) {text}"
        else:
            return text
    df["Circular / Direction"] = df["Circular / Direction"].apply(prefix_pdf_name)
    def _fill_currency(row):
        cur = row.get("currency")
        if pd.isna(cur) or not str(cur).strip() or str(cur) == "nan":
            penalty_text = "" if pd.isna(row.get("penalty_amount_text")) else str(row.get("penalty_amount_text"))
            reason_text = "" if pd.isna(row.get("reason_text")) else str(row.get("reason_text"))
            summary_text = "" if pd.isna(row.get("summary_sentence")) else str(row.get("summary_sentence"))
            blob = " ".join([penalty_text, reason_text, summary_text])
            cur = detect_currency(blob)
        return cur
    df["currency"] = df.apply(_fill_currency, axis=1)
    df["Min Penalty"], df["Max Penalty"], df["Penalty Range"] = zip(*df.apply(extract_amounts, axis=1))
    # Always fill missing Legal Provision Invoked using fallback
    df["Legal Provision Invoked"] = df.apply(extract_legal_from_row, axis=1)
    # Always fill missing Reason / Description with context if empty
    def fill_reason(row):
        val = row.get("Reason / Description")
        if pd.isna(val) or not str(val).strip():
            # Try to use penalty_amount_text or context as fallback
            penalty_text = row.get("penalty_amount_text", "")
            if penalty_text and not pd.isna(penalty_text):
                return penalty_text
            # fallback to context if available
            context = row.get("context", "")
            if context and not pd.isna(context):
                return context
            return ""
        return val
    if "Reason / Description" in df.columns:
        df["Reason / Description"] = df.apply(fill_reason, axis=1)
    df.rename(columns={"id": "SL No", "reason_text": "Reason / Description"}, inplace=True)
    df["SL No"] = range(1, len(df) + 1)
    final_cols = [
        "SL No", "Circular / Direction", "Violation Type", "Penalty Range",
        "Legal Provision Invoked", "Reason / Description", "Page"
    ]
    df_final = df[final_cols]
    # Replace all nan/NA with empty string for UI
    df_final = df_final.replace([pd.NA, np.nan, "nan"], "")
    # Log the final DataFrame in table format
    logging.info(f"Final output table:\n{df_final.to_string(index=False)}")
    return df_final