# RATP Data Engineering Pipeline

[![RATP Data Pipeline CI](https://github.com/ouladouayerefrancis/ratp_data_pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/ouladouayerefrancis/ratp_data_pipeline/actions/workflows/ci.yml)

Pipeline Data Engineering en Python construit autour d'une architecture :

**RAW → Bronze → Data Quality → Quarantaine / Audit → Silver → Gold**

> Projet pédagogique et portfolio utilisant uniquement des données synthétiques.  
> Ce dépôt n'est pas un projet officiel de la RATP et n'utilise aucune donnée confidentielle de l'entreprise.

## Objectifs

Le projet met en pratique :

- ingestion de fichiers CSV ;
- historisation Bronze ;
- idempotence ;
- Data Quality ;
- détection des doublons exacts et conflictuels ;
- audit et quarantaine ;
- RCA et remédiation ;
- replay contrôlé ;
- construction Silver ;
- agrégations Gold ;
- Quality Gates ;
- réconciliation Bronze / Silver / Gold ;
- logging ;
- CLI ;
- tests automatisés avec pytest ;
- intégration continue avec GitHub Actions.

## Architecture

```text
RAW
 │
 ▼
BRONZE
 │
 ▼
DATA QUALITY
 ├── PASS
 ├── EXACT_DUPLICATE
 └── CONFLICTING_DUPLICATE
        │
        ├── AUDIT
        └── QUARANTINE
                │
                ▼
          RCA / REMEDIATION
                │
                ▼
          REPLAY CONTRÔLÉ
                │
                ▼
          SILVER FINAL
                │
                ▼
          QUALITY GATES
                │
                ▼
              GOLD