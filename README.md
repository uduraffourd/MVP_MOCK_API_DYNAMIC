# HPP MVP API (CSV-backed FastAPI)

Mini-backend **FastAPI** qui expose des données de production hydroélectrique depuis un **CSV**, comme si c’était une base de données.  
Objectif : fournir une API mock réaliste avant de brancher une vraie DB et un front-end UI.

## Structure
.
├─ main.py          # Entrypoint (expose app + lance Uvicorn)
├─ api.py           # App factory, modèles Pydantic, routes, agrégations
├─ fake_db.py       # Abstraction DataStore + implémentation CSV
├─ requirements.txt # Dépendances Python
└─ README.md        # Ce fichier


# Méthode 1 : via Uvicorn directement
uvicorn main:app --reload --port 4010