# 1) Crie e ative um venv
    python -m venv .venv
    # Windows: .venv\\Scripts\\activate
    # macOS/Linux: source .venv/bin/activate
# 2) pip install -r requirements.txt
# 3) Estrutura esperada:
    socios/
      src/
        __init__.py 
        config.py
        download.py
        extract.py
        merge.py
        main.py
      data/ (será criada automaticamente)
# 4) Executar (a partir da raiz):
    python -m src.main baixar            # baixa todo o intervalo padrão (2024-08..2025-10)
    python -m src.main baixar 2024-08    # baixa só um mês
    python -m src.main baixar 2024-08 2024-10  # baixa um intervalo
    python -m src.main extrair           # extrai todo o intervalo (stub ok)
    python -m src.main consolidar        # consolida (stub)
    python -m src.main tudo              # pipeline completo (com consolidação stub)
    
## Próxima etapa (v1):
###  - Implementar merge/consolidação com Polars (CSV sem header -> schema -> Parquet)
###  - (opcional) Inserção em SQLite e criação de índices.
