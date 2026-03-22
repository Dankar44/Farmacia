CREATE OR REPLACE FUNCTION public.search_products(search_term text, page_num integer DEFAULT 1, page_size integer DEFAULT 50, sort_by text DEFAULT 'relevancia')
RETURNS json
LANGUAGE plpgsql
AS $$
DECLARE
    norm TEXT;
    result JSON;
    cnt INT;
    order_clause TEXT;
BEGIN
    norm := regexp_replace(lower(unaccent(search_term)), '[^a-z0-9]', '', 'g');

    SELECT COUNT(DISTINCT nombre_normalizado) INTO cnt
    FROM busqueda_rapida WHERE nombre_normalizado LIKE '%' || norm || '%';

    IF cnt = 0 THEN RETURN json_build_object('total', 0, 'items', '[]'::json); END IF;

    -- Build order clause based on sort_by
    CASE sort_by
        WHEN 'nombre-asc' THEN order_clause := 'MIN(b.nombre) ASC';
        WHEN 'nombre-desc' THEN order_clause := 'MIN(b.nombre) DESC';
        WHEN 'precio-asc' THEN order_clause := 'MIN(b.precio) ASC NULLS LAST';
        WHEN 'precio-desc' THEN order_clause := 'MIN(b.precio) DESC NULLS LAST';
        WHEN 'farmacias-desc' THEN order_clause := 'COUNT(*) DESC, MIN(b.precio) ASC NULLS LAST';
        WHEN 'stock' THEN order_clause := 'BOOL_OR(b.en_stock) DESC, MIN(b.precio) ASC NULLS LAST';
        ELSE order_clause := 'MIN(b.nombre) ASC';
    END CASE;

    EXECUTE format(
        'SELECT json_build_object(''total'', %s, ''items'', json_agg(grp))
        FROM (
            SELECT json_build_object(
                ''nombre'', MIN(b.nombre),
                ''mejor_precio'', MIN(b.precio),
                ''num_farmacias'', COUNT(*),
                ''en_stock'', BOOL_OR(b.en_stock),
                ''ean'', MAX(b.ean),
                ''farmacias'', json_agg(json_build_object(
                    ''farmacia'', b.farmacia,
                    ''precio'', b.precio,
                    ''en_stock'', b.en_stock,
                    ''url'', b.url
                ) ORDER BY b.precio ASC NULLS LAST)
            ) as grp
            FROM busqueda_rapida b
            WHERE b.nombre_normalizado LIKE ''%%'' || %L || ''%%''
            GROUP BY b.nombre_normalizado
            ORDER BY %s
            LIMIT %s OFFSET %s
        ) sub',
        cnt, norm, order_clause, page_size, (page_num - 1) * page_size
    ) INTO result;

    RETURN result;
END;
$$;
