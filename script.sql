CREATE OR REPLACE VIEW view_taxa_variacao_por_grupo AS
WITH taxa_base AS (
    SELECT
        ds.nome AS servico,
        dg.nome AS grupo_economico,
        dd.data_referencia::DATE AS data,
        f.valor AS taxa_variacao
    FROM fato_ida f
    JOIN dim_servico ds ON f.servico_id = ds.id
    JOIN dim_grupo_economico dg ON f.grupo_economico_id = dg.id
    JOIN dim_data dd ON f.data_id = dd.id
    JOIN dim_variavel dv ON f.variavel_id = dv.id
    WHERE dv.nome = 'taxa de resolvidas em 5 dias úteis'
),
taxa_com_media AS (
    SELECT
        servico,
        data,
        grupo_economico,
        taxa_variacao,
        AVG(taxa_variacao) OVER (PARTITION BY servico, data) AS taxa_variacao_media
    FROM taxa_base
),
taxa_final AS (
    SELECT
        servico,
        data,
        taxa_variacao_media,
        grupo_economico,
        ROUND((taxa_variacao - taxa_variacao_media)::numeric, 2) AS diferenca
    FROM taxa_com_media
)
SELECT
    servico,
    data,
    taxa_variacao_media,
    MAX(CASE WHEN LOWER(grupo_economico) = 'algar' THEN diferenca END) AS algar,
    MAX(CASE WHEN LOWER(grupo_economico) = 'net' THEN diferenca END) AS net,
    MAX(CASE WHEN LOWER(grupo_economico) = 'oi' THEN diferenca END) AS oi,
    MAX(CASE WHEN LOWER(grupo_economico) = 'sercomtel' THEN diferenca END) AS sercomtel,
    MAX(CASE WHEN LOWER(grupo_economico) = 'sky' THEN diferenca END) AS sky,
    MAX(CASE WHEN LOWER(grupo_economico) = 'tim' THEN diferenca END) AS tim,
    MAX(CASE WHEN LOWER(grupo_economico) = 'vivo' THEN diferenca END) AS vivo,
    MAX(CASE WHEN LOWER(grupo_economico) = 'claro tv' THEN diferenca END) AS claro_tv,
    MAX(CASE WHEN LOWER(grupo_economico) = 'ctbc tv' THEN diferenca END) AS ctbc_tv,
    MAX(CASE WHEN LOWER(grupo_economico) = 'gvt' THEN diferenca END) AS gvt,
    MAX(CASE WHEN LOWER(grupo_economico) = 'oi tv' THEN diferenca END) AS oi_tv,
    MAX(CASE WHEN LOWER(grupo_economico) = 'telefônica vivo' THEN diferenca END) AS telefonica_vivo,
    MAX(CASE WHEN LOWER(grupo_economico) = 'viacabo' THEN diferenca END) AS viacabo,
    MAX(CASE WHEN LOWER(grupo_economico) = 'claro' THEN diferenca END) AS claro,
    MAX(CASE WHEN LOWER(grupo_economico) = 'embratel' THEN diferenca END) AS embratel,
    MAX(CASE WHEN LOWER(grupo_economico) = 'nextel' THEN diferenca END) AS nextel,
    MAX(CASE WHEN LOWER(grupo_economico) = 'ctbc celular' THEN diferenca END) AS ctbc_celular,
    MAX(CASE WHEN LOWER(grupo_economico) = 'oi celular' THEN diferenca END) AS oi_celular,
    MAX(CASE WHEN LOWER(grupo_economico) = 'sercomtel celular' THEN diferenca END) AS sercomtel_celular,
    MAX(CASE WHEN LOWER(grupo_economico) = 'tim gsm' THEN diferenca END) AS tim_gsm,
    MAX(CASE WHEN LOWER(grupo_economico) = 'ctbc telecom' THEN diferenca END) AS ctbc_telecom,
    MAX(CASE WHEN LOWER(grupo_economico) = 'telefônica' THEN diferenca END) AS telefonica,
    MAX(CASE WHEN LOWER(grupo_economico) = 'tim/intelig' THEN diferenca END) AS tim_intelig
FROM taxa_final
GROUP BY servico, data, taxa_variacao_media
ORDER BY servico, data;

