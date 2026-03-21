CREATE OR REPLACE FUNCTION test_normalize(input_string TEXT)
RETURNS TEXT AS $$
DECLARE
    clean_str TEXT;
BEGIN
    -- lower and unaccent
    clean_str := unaccent(lower(input_string));
    
    -- Separate numbers and letters with a space (e.g., '60caps' -> '60 caps', '2x10' -> '2 x 10')
    clean_str := regexp_replace(clean_str, '([0-9])([a-z])', '\1 \2', 'g');
    clean_str := regexp_replace(clean_str, '([a-z])([0-9])', '\1 \2', 'g');
    
    -- Strip common noise words (units, formats, etc) completely
    -- Using \y for word boundaries so we don't accidentally remove parts of other words
    -- caps, capsula, capsulas, cap
    clean_str := regexp_replace(clean_str, '\y(capsulas?|caps?)\y', '', 'g');
    -- comp, comprimido, comprimidos
    clean_str := regexp_replace(clean_str, '\y(comprimidos?|comp?)\y', '', 'g');
    -- pack, envase, duplo, triplo, etc
    clean_str := regexp_replace(clean_str, '\y(pack|envase|duplo|triplo)\y', '', 'g');
    -- x (used as multiplier) 
    clean_str := regexp_replace(clean_str, '\yx\y', '', 'g');
    -- ml, mg, gr, g, l
    clean_str := regexp_replace(clean_str, '\y(ml|mg|gr|g|l|kg)\y', '', 'g');
    -- solucion, gotas, ocular can be tricky if the brand is 'solucion', but for hyabak it's fluff.
    -- Let's stick to standardizing units first!
    
    -- remove all non-alphanumeric
    clean_str := regexp_replace(clean_str, '[^a-z0-9]', '', 'g');
    
    RETURN clean_str;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

SELECT nombre, test_normalize(nombre) 
FROM productos 
WHERE nombre ILIKE '%hyabak%' 
ORDER BY test_normalize(nombre);
