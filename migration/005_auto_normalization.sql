-- ============================================================
-- Migration 005: Auto-normalization trigger + display name
-- ============================================================

-- A. Add nombre_display column
ALTER TABLE productos ADD COLUMN IF NOT EXISTS nombre_display VARCHAR(500);

-- B. Improved normalize_product_name() — safer unit stripping
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

    -- Strip known pharmacy prefixes before normalization
    clean_str := regexp_replace(clean_str, '^\s*(promofarma|atida|mifarma|dosfarma|farmacia\s*barata|okfarma|farmacoslada|farmacias\s*direct|farmacias\s*vazquez)\s*[-:|/·]\s*', '', 'i');

    -- Separate numbers and letters (e.g., '60caps' -> '60 caps', '50ml' -> '50 ml')
    clean_str := regexp_replace(clean_str, '([0-9])([a-z])', '\1 \2', 'g');
    clean_str := regexp_replace(clean_str, '([a-z])([0-9])', '\1 \2', 'g');

    -- Strip common noise words
    clean_str := regexp_replace(clean_str, '\y(capsulas?|caps?)\y', '', 'g');
    clean_str := regexp_replace(clean_str, '\y(comprimidos?|comp?)\y', '', 'g');

    -- Strip multiplier 'x'
    clean_str := regexp_replace(clean_str, '\yx\y', '', 'g');

    -- Strip units ONLY when preceded by a number (safer — won't eat "Vitamin G")
    clean_str := regexp_replace(clean_str, '([0-9])\s*(ml|mg|gr|kg)\y', '\1', 'g');
    -- 'g' and 'l' standalone are risky, only strip after number
    clean_str := regexp_replace(clean_str, '([0-9])\s*(g|l)\y', '\1', 'g');

    -- Semantic strip: brand prefixes that act as noise
    clean_str := regexp_replace(clean_str, '\y(thea|laboratorios)\y', '', 'g');

    -- Remove all non-alphanumeric
    clean_str := regexp_replace(clean_str, '[^a-z0-9]', '', 'g');

    RETURN clean_str;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- C. Display name function
CREATE OR REPLACE FUNCTION clean_display_name(raw_name TEXT)
RETURNS TEXT AS $$
DECLARE
    clean TEXT;
    upper_count INT;
    total_count INT;
BEGIN
    clean := raw_name;

    -- Strip pharmacy prefixes (case-insensitive)
    clean := regexp_replace(clean,
        '^\s*(PromoFarma|Atida|Mifarma|DosFarma|Farmacia\s*Barata|OkFarma|Farmacoslada|Farmacias\s*Direct|Farmacias\s*Vazquez)\s*[-:|/·]\s*',
        '', 'i');

    -- Trim leading/trailing whitespace
    clean := trim(clean);

    -- If mostly uppercase (>60%), convert to title case
    total_count := length(regexp_replace(clean, '[^a-zA-Z]', '', 'g'));
    upper_count := length(regexp_replace(clean, '[^A-Z]', '', 'g'));

    IF total_count > 0 AND upper_count::float / total_count > 0.6 THEN
        clean := initcap(lower(clean));
    ELSE
        -- Just capitalize the first letter
        clean := upper(left(clean, 1)) || substring(clean from 2);
    END IF;

    -- Collapse multiple spaces
    clean := regexp_replace(clean, '\s+', ' ', 'g');

    RETURN clean;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- D. Trigger function
CREATE OR REPLACE FUNCTION trigger_normalize_product()
RETURNS TRIGGER AS $$
BEGIN
    NEW.nombre_normalizado := normalize_product_name(NEW.nombre);
    NEW.nombre_display := clean_display_name(NEW.nombre);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_normalize_product ON productos;
CREATE TRIGGER trg_normalize_product
    BEFORE INSERT OR UPDATE OF nombre ON productos
    FOR EACH ROW
    EXECUTE FUNCTION trigger_normalize_product();

-- E. Backfill existing data
UPDATE productos
SET nombre_normalizado = normalize_product_name(nombre),
    nombre_display = clean_display_name(nombre);

-- F. Index on nombre_display
CREATE INDEX IF NOT EXISTS idx_productos_nombre_display ON productos(nombre_display);
