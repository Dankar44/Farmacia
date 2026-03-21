CREATE OR REPLACE FUNCTION normalize_product_name(input_string TEXT)
RETURNS TEXT AS $$
DECLARE
    clean_str TEXT;
BEGIN
    -- lower and unaccent
    clean_str := unaccent(lower(input_string));
    
    -- Normalize weird multiplier symbols (e.g. math times symbol or asterisks) to the letter 'x'
    clean_str := replace(clean_str, '×', 'x');
    clean_str := replace(clean_str, '*', 'x');
    
    -- Separate numbers and letters with a space (e.g., '60caps' -> '60 caps', '2x10' -> '2 x 10')
    clean_str := regexp_replace(clean_str, '([0-9])([a-z])', '\1 \2', 'g');
    clean_str := regexp_replace(clean_str, '([a-z])([0-9])', '\1 \2', 'g');
    
    -- Strip common noise words (units, formats, etc) completely
    -- caps, capsula, capsulas, cap
    clean_str := regexp_replace(clean_str, '\y(capsulas?|caps?)\y', '', 'g');
    -- comp, comprimido, comprimidos
    clean_str := regexp_replace(clean_str, '\y(comprimidos?|comp?)\y', '', 'g');
    
    -- REMOVED: stripping of 'pack', 'envase', 'duplo', 'triplo' to preserve variant distinctness
    
    -- x (used as multiplier) 
    clean_str := regexp_replace(clean_str, '\yx\y', '', 'g');
    -- ml, mg, gr, g, kg, l
    clean_str := regexp_replace(clean_str, '\y(ml|mg|gr|g|kg|l)\y', '', 'g');
    
    -- Semantic strip: Remove specific brand prefixes that act as noise (e.g., 'Thea', 'Laboratorios Thea')
    clean_str := regexp_replace(clean_str, '\y(thea|laboratorios)\y', '', 'g');
    
    -- remove all non-alphanumeric
    clean_str := regexp_replace(clean_str, '[^a-z0-9]', '', 'g');
    
    RETURN clean_str;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Renormalize the whole database
UPDATE productos SET nombre_normalizado = normalize_product_name(nombre);
