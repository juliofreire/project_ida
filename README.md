# Projeto IDA - Processo Seletivo

Esse repositório foi desenvolvido para a etapa do processo seletivo da **BeAnalytic**.

São utilizados containers em uma aplicação Docker:

- **ETL (Python 3.11.12 - bookworm)** - Faz todo o processo de extração, transformação e carga dos dados;
- **PostgresSQL (17.5 - bookworm)** - Armazena os dados carregados pelo ETL.

Todo o ETL está dividido em scripts para cada uma das etapas e são coordenados pela função main.py, assim ele se torna escalável e de fácil manutenção.

## Como utilizar

```bash
git clone https://github.com/juliofreire/project_ida.git

cd project_ida

sudo docker compose up --build 


```

OBS.: A parte de criação da view não ficou completa, faltou entendimento do que era de fato pedido, apesar de ter sido feito uma view com as informações erradas. Ela também não foi automatizada com os containers, então os dados são extraídos, transformados e carregados, porém a view não é criada automaticamente pelo script ./backend/create_view.sql