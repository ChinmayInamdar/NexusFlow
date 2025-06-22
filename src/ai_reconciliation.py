# src/ai_reconciliation.py
import os
import re
import json
import google.generativeai as genai
import getpass
from .config import logger, GEMINI_API_KEY

gemini_model = None
gemini_api_key_provided = False

def configure_gemini():
    global gemini_model, gemini_api_key_provided
    
    if gemini_model is not None:
        # logger.info("Gemini model already configured.") # Can be too verbose
        return gemini_model

    try:
        api_key = GEMINI_API_KEY # From config
        if not api_key:
            logger.info("GEMINI_API_KEY environment variable (via config) not found.")
            try:
                api_key_input = getpass.getpass("Enter your Gemini API Key (or press Enter to skip AI features): ")
                if not api_key_input:
                    logger.warning("No Gemini API Key provided. AI-assisted features will be skipped.")
                    gemini_api_key_provided = False
                    return None
                api_key = api_key_input
            except RuntimeError: # e.g. if getpass cannot be used in the environment
                logger.warning("Cannot prompt for API key in this environment. Set GEMINI_API_KEY env var. AI features skipped.")
                gemini_api_key_provided = False
                return None

        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-1.5-pro-latest')
        gemini_model = model
        gemini_api_key_provided = True
        logger.info("Gemini 1.5 Pro model configured successfully.")
        return model
    except Exception as e:
        logger.error(f"Error configuring Gemini: {e}. AI-assisted features may not work.")
        gemini_api_key_provided = False
        return None

def get_ai_schema_mapping_suggestions(source_name_a, columns_a, source_name_b, columns_b, target_schema_dict):
    global gemini_model # Ensure we are using the module-level model

    if not gemini_api_key_provided or not gemini_model:
        # Attempt to configure if it hasn't been tried or failed before
        if not gemini_model and not gemini_api_key_provided: # Only configure if not already flagged as no-key
            configure_gemini()
        
        if not gemini_model: # Still not configured
            logger.warning("Gemini model not configured. Skipping AI schema mapping.")
            return None
        
    prompt_parts = [
        "You are a data engineering assistant specializing in schema mapping and reconciliation.",
        "Given column names from two different source dataframes and a target canonical database schema, ",
        "your task is to suggest the most likely mappings from each source's columns to the target schema's columns.",
        "Consider common naming conventions, abbreviations, and semantic similarities (e.g., 'cust_id' and 'customer_identifier' might both map to 'customer_id').",
        "\n--- Source A Details ---",
        f"Source A Name: {source_name_a}",
        f"Source A Columns: {', '.join(columns_a)}",
        "\n--- Source B Details ---",
        f"Source B Name: {source_name_b}",
        f"Source B Columns: {', '.join(columns_b)}",
        "\n--- Target Canonical Schema ---"
    ]
    for table, cols in target_schema_dict.items():
        prompt_parts.append(f"Target Table '{table}': {', '.join(cols)}")

    prompt_parts.append("\n--- Instructions ---")
    prompt_parts.append(f"1. For '{source_name_a}', provide mappings to any relevant target table and column.")
    prompt_parts.append(f"2. For '{source_name_b}', provide mappings to any relevant target table and column.")
    prompt_parts.append("3. If a source column does not seem to map to any target column, indicate with 'NO_CLEAR_TARGET'.")
    prompt_parts.append("4. If a source column could map to multiple targets, list the most probable or note the ambiguity.")
    prompt_parts.append("Provide the output strictly as a single JSON object. Do not include any text or markdown formatting before or after the JSON block.")
    prompt_parts.append("\nExample JSON output format:")
    prompt_parts.append("""
    {
      "source_a_mappings": {
        "client_ref": "Customers.customer_id",
        "purchase_dt": "Orders.order_date",
        "item_sku": "Products.product_id",
        "weird_legacy_col": "NO_CLEAR_TARGET"
      },
      "source_b_mappings": {
        "CustomerID": "Customers.customer_id",
        "orderTimestamp": "Orders.order_date",
        "productCode": "Products.product_id"
      }
    }
    """)
    
    prompt = "\n".join(prompt_parts)
    # logger.info(f"Sending schema mapping prompt to Gemini (first 500 chars):\n{prompt[:500]}...") # Can be verbose
    
    try:
        response = gemini_model.generate_content(prompt)
        
        try:
            # Gemini might wrap JSON in ```json ... ```, or just ``` ... ```
            match = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", response.text, re.DOTALL)
            if match:
                json_str = match.group(1).strip()
            else:
                # If no backticks, try to find the first '{' and last '}'
                first_brace = response.text.find('{')
                last_brace = response.text.rfind('}')
                if first_brace != -1 and last_brace != -1 and last_brace > first_brace:
                    json_str = response.text[first_brace : last_brace+1].strip()
                else:
                    json_str = response.text.strip() # Assume it's raw JSON if no other structure found
            
            suggestions = json.loads(json_str)
            logger.info("Successfully received and parsed schema mapping suggestions from Gemini.")
            return suggestions
        except json.JSONDecodeError as e_json:
            logger.error(f"Gemini response was not valid JSON. JSON Error: {e_json}. Raw response (first 500 chars):\n{response.text[:500]}")
            return {"error": "Invalid JSON response", "raw_text": response.text}
        except Exception as e_parse: # Catch other parsing/unexpected issues
            logger.error(f"Error processing Gemini response text: {e_parse}. Raw response (first 500 chars):\n{response.text[:500]}")
            return {"error": f"Processing error: {str(e_parse)}", "raw_text": response.text}

    except Exception as e_api: # Catch API call errors (like rate limits)
        logger.error(f"Error calling Gemini API for schema mapping: {type(e_api).__name__} - {e_api}")
        # Check if the error is due to rate limiting or other API issues
        if "quota" in str(e_api).lower() or "rate limit" in str(e_api).lower():
            logger.warning("Gemini API quota exceeded or rate limit hit.")
        return None # Indicate failure to get suggestions

logger.info("AI reconciliation utilities defined in src/ai_reconciliation.py.")