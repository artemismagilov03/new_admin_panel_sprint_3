SELECT to_json(sub_query)
FROM (SELECT id   AS id,
             name AS name
      FROM genre
      WHERE updated_at > NOW()
      ORDER BY id) AS sub_query;
