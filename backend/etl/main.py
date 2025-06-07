from extractor import Extractor
from transformer import Transformer
from loader import Loader

if __name__ == "__main__":

    extractor = Extractor(
        api_base_url="https://dados.gov.br/api/publico/conjuntos-dados/indice-desempenho-atendimento",
        download_path="/data/raw"
    )
    extractor.run()

    transformer = Transformer(
        input_path="/data/raw",
        output_file="/data/processed/dados_tratados.csv"
    )
    transformer.run()

    loader = Loader(
        db_url="postgresql+psycopg2://julio:123456@db/project_ida",
        input_file="/data/processed/dados_tratados.csv"
    )

    loader.run()
