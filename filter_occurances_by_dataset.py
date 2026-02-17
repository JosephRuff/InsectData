import duckdb

con = duckdb.connect(database=":memory:")

con.execute("""
COPY (
    SELECT
        datasetKey,
        year,
        month,
        COUNT (DISTINCT(speciesKey)) AS speciesRichness,
        COUNT(*) AS occurrenceCount
    FROM read_csv(
        'occurrence.txt',
        delim='\t'
    )
    WHERE
        occurrenceStatus = 'PRESENT' AND
        year IS NOT NULL AND
        month IS NOT NULL
    GROUP BY
        datasetKey, 
        year,
        month
) TO 'occurrence_monthly_counts_by_dataset.csv'
(FORMAT CSV);
""")
