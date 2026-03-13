CREATE OR REPLACE TABLE `p1422-477909.INTEGRATION.integration_prueba_tecnica` (
    id INT64 NOT NULL,
    userId INT64 NOT NULL,
    title STRING,
    body STRING,
    LOAD_DATE DATE NOT NULL
) AS
SELECT
    id,
    userId,
    title,
    body,
    CURRENT_DATE() AS LOAD_DATE
FROM `p1422-477909.SANDBOX_jsonplaceholder.posts_raw`
GROUP BY 1,2,3,4;

/*
Se ha decidido utilizar CREATE OR REPLACE TABLE para cumplir con el requisito de utilizar una única consulta.
Si crear la tabla antes de ejecutar la consulta está permitido, utilizaría MERGE para insertar únicamente nuevos datos:
*/

MERGE `p1422-477909.INTEGRATION.integration_prueba_tecnica` AS target
USING (
    SELECT
        id,
        userId,
        title,
        body,
        CURRENT_DATE() as LOAD_DATE
    FROM `p1422-477909.SANDBOX_jsonplaceholder.posts_raw`
    GROUP BY 1,2,3,4
) AS source
ON target.id = source.id
WHEN MATCHED THEN
    UPDATE SET
        target.userId = source.userId,
        target.title = source.title,
        target.body = source.body,
        target.load_date = source.LOAD_DATE
WHEN NOT MATCHED THEN
    INSERT (
        id,
        userId,
        title,
        body,
        LOAD_DATE
    )
    VALUES (
        source.id,
        source.userId,
        source.title,
        source.body,
        source.LOAD_DATE
    );

/*
source se define con un SELECT GROUP BY para asegurar que no hay duplicados.
En esta consulta se asume que todos los duplicados son exactos (por ejemplo, producto de cargar los datos de la API dos veces).
Si existiesen dos registros con el mismo valor en id pero distinto en alguna de las otras 3 columnas de posts_raw, cosa que no puede pasar con esta API, fallaría.
*/