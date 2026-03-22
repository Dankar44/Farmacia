-- NOTE: This function is superseded by migration/005_auto_normalization.sql
-- which includes the trigger and clean_display_name(). Use that migration instead.
-- This file is kept for reference.

CREATE OR REPLACE FUNCTION normalize_product_name(input_string TEXT)
RETURNS TEXT AS $$
DECLARE
    clean_str TEXT;
BEGIN
    -- lower and unaccent
    clean_str := unaccent(lower(input_string));

    -- Normalize multiplier symbols
    clean_str := replace(clean_str, '×', 'x');
    clean_str := replace(clean_str, '*', 'x');

    -- Strip known pharmacy prefixes
    clean_str := regexp_replace(clean_str, '^\s*(promofarma|atida|mifarma|dosfarma|farmacia\s*barata|okfarma|farmacoslada|farmacias\s*direct|farmacias\s*vazquez)\s*[-:|/·]\s*', '', 'i');

    -- Separate numbers and letters (e.g., '60caps' -> '60 caps', '50ml' -> '50 ml')
    clean_str := regexp_replace(clean_str, '([0-9])([a-z])', '\1 \2', 'g');
    clean_str := regexp_replace(clean_str, '([a-z])([0-9])', '\1 \2', 'g');

    -- Strip common noise words
    clean_str := regexp_replace(clean_str, '\y(capsulas?|caps?)\y', '', 'g');
    clean_str := regexp_replace(clean_str, '\y(comprimidos?|comp?)\y', '', 'g');

    -- Strip multiplier 'x'
    clean_str := regexp_replace(clean_str, '\yx\y', '', 'g');

    -- Strip units ONLY when preceded by a number (safer)
    clean_str := regexp_replace(clean_str, '([0-9])\s*(ml|mg|gr|kg)\y', '\1', 'g');
    clean_str := regexp_replace(clean_str, '([0-9])\s*(g|l)\y', '\1', 'g');

    -- Semantic strip: brand prefixes
    clean_str := regexp_replace(clean_str, '\y(thea|laboratorios)\y', '', 'g');

    -- Remove all non-alphanumeric
    clean_str := regexp_replace(clean_str, '[^a-z0-9]', '', 'g');

    RETURN clean_str;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Renormalize the whole database
UPDATE productos SET nombre_normalizado = normalize_product_name(nombre);
