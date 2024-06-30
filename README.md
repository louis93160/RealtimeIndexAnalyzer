<h2 align="center" style="font-size: 20px;">Analyse en temps réel des données d'indices boursiers</h2>

<div align="center" style="font-size: 22px;">Projet Data Engineering - Louis Duvieux</div>

<br><br>

<p align="center">
  <img src="https://icon.icepanel.io/Technology/svg/Python.svg" alt="Python" height="80">
  <img src="https://icon.icepanel.io/Technology/svg/Java.svg" alt="Java" height="80">
  <img src="https://icon.icepanel.io/Technology/svg/Apache-Kafka.svg" alt="Kafka" height="80">
  <img src="https://icon.icepanel.io/Technology/svg/Apache-Spark.svg" alt="Apache Spark" height="80">
  <img src="https://icon.icepanel.io/Technology/svg/PostgresSQL.svg" alt="PostgreSQL" height="80">
</p>


## Description du projet

Pour ses équipes opérant sur les marchés financiers, une société d'investissement a besoin de proposer une base de données qui stocke des informations financières sur les indices boursiers des sociétés cotées au NASDAQ. En particulier, elle souhaite pouvoir proposer à ces équipes des calculs agrégés sur les indices du NASDAQ, et en utilisant des fenêtres temporelles. En tant que Data Engineer, elle vous demande de construire un job d'agrégation de données à partir des valeurs stockées en temps réel sur un cluster Kafka. Le job doit pouvoir être exécuté en continu sur un serveur et doit être en mesure d'alimenter une base PostgreSQL où les valeurs seront mises à jour en temps réel.

Le cluster Kafka est déjà en place, et celui-ci contient un topic stock-data où, à chaque seconde, l'information de clôture d'un symbole (ou tracker) est envoyé dessus par un programme tiers. Les données stockées sur ce topic sont disponibles au format JSON et selon le schéma suivant :

```json
{
  "Adj Close": 8.399900436401367,
  "Close": 8.399900436401367,
  "Datetime": "2023-10-03T15:26:23",
  "Symbol": "ZYXI",
  "Volume": 1337.0
}
```

- `Close` correspond à la valeur de fermeture brute (celle qui sera utilisée par défaut).
- `Adj Close` correspond à la valeur de fermeture en ayant pris en compte ajusté selon les arbitrages opérés sur cet indice (dividendes et stock-options).
- `Datetime` correspond à la fin de la fenêtre temporelle à partir de laquelle est calculée la valeur de fermeture.
- `Symbol` correspond au symbole du tracker.
- `Volume` correspond au volume d'échange réalisé sur ce tracker.

## Contraintes

L'entreprise n'oblige aucun framework à utiliser pour effectuer les calculs. Néanmoins, il est conseillé d'utiliser Kafka Streams, Spark Structure Streaming ou Apache Flink pour faciliter les calculs d'agrégations et les intégrations avec PostgreSQL.

## Tables SQL

La compagnie demande à ce que le format en sortie du job respecte le schéma utilisé dans la table PostgreSQL. Pour cela, elle fournit le code SQL de création de la table tickers dans la base de données stocks.

```sql
CREATE TABLE IF NOT EXISTS public.tickers
(
    symbol text NOT NULL,
    low_1m double precision,
    low_5m double precision,
    high_1m double precision,
    high_5m double precision,
    avg_1m double precision,
    avg_5m double precision,
    diff_1m double precision,
    diff_5m double precision,
    rate_1m double precision,
    rate_5m double precision,
    CONSTRAINT tickers_pkey PRIMARY KEY (symbol)
)
```

La table stocke des informations agrégées sur des fenêtres d'1 minute (_1m) et de 5 minutes (_5m). Les informations agrégées demandées sont les suivantes :

- `low` : la valeur minimale atteinte sur la fenêtre temporelle.
- `high` : la valeur maximale atteinte sur la fenêtre temporelle.
- `avg` : la valeur moyenne atteinte sur la fenêtre temporelle.
- `diff` : l'écart maximal entre la plus petite et la plus grande valeur de clôture sur la fenêtre temporelle.
- `rate` : le rendement maximal théorique possible sur la fenêtre temporelle.

La formule du rendement maximal théorique est la suivante : 

Rate = High - Low / Low
Ainsi, `low_5m` permet de connaître la valeur minimale atteinte sur les 5 dernière minutes.



