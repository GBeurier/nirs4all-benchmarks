# nirs4all-arena design

| Champ | Valeur |
|---|---|
| Statut | Design initial nettoye |
| Date | 2026-06-05 |
| Audience | Mainteneurs nirs4all, futurs contributeurs de datasets/methodes, developpeurs Arena |
| Perimetre | Stockage, ingestion, requetage et dataviz des performances de pipelines NIRS |
| Hors perimetre | Execution compute detaillee, hebergement de datasets, conservation de modeles entraines |

## 1. Resume

`nirs4all-arena` doit devenir l'environnement de reference pour stocker, comparer et explorer les performances de pipelines NIRS. L'Arena ne stocke pas les datasets bruts, les modeles entraines, les bundles `.n4a`, les transformeurs fitted ou autres artefacts lourds. Elle stocke:

- les cartes d'identite des datasets ou fingerprints anonymises;
- les definitions completes des taches, splits, CV, seeds et RNG;
- les pipelines nirs4all sous forme canonique, y compris les DAG complexes;
- les scores versionnes;
- les residus et, seulement si autorise, les predictions/targets necessaires au recalcul;
- les metadonnees d'environnement suffisantes pour documenter les conditions d'execution.

Le tuple experimental de base est:

```text
DATASET_CARD ou DATASET_FINGERPRINT
  x TASK
  x DATASET_VARIANT
  x METHOD_SPLIT_TEST
  x METHOD_CV
  x RNG_CONTEXT
  x PIPELINE_DAG
  x REFIT_STRATEGY
  x SCORE_VERSION
```

`PIPELINE_DAG` remplace l'ancienne vision lineaire `preprocessing_chain x model`: un pipeline peut contenir des branches, des traitements multi-source ou multimodaux, des merges, plusieurs modeles, du stacking, du bagging, une moyenne, un vote, ou toute autre structure DAG produite par nirs4all/dag-ml.

L'Arena doit servir deux usages:

1. **Benchmarks publics** sur les datasets de reference de `nirs4all-datasets`.
2. **Agregation de runs utilisateurs** a moyen terme, meme si le dataset n'est pas partageable: on stocke alors une carte statistique anonymisee et un hash/fingerprint, pas les donnees.

## 2. Decisions structurantes

1. **Producteur cible unique: nirs4all.** Les runs viennent a terme de nirs4all, quelle que soit la capsule: Python lib, R lib, Studio, Studio Lite, batch/cluster. Les sources externes ne sont acceptees que si elles passent par un export/manifest compatible nirs4all.
2. **Pas d'artefacts.** L'Arena ne conserve pas les modeles entraines, transformeurs fitted, caches de features, bundles ou datasets. Elle conserve les pipelines documentes, scores, residus et cartes d'identite.
3. **Stockage aligne workspace nirs4all.** Le format cible est SQLite pour les metadonnees et Parquet pour les arrays de resultats/residus, en continute avec le workspace nirs4all actuel. DuckDB n'est pas le format de reference.
4. **Dataset decouple en trois niveaux.** `DatasetCard` decrit les donnees, `TaskSpec` decrit la cible/tache, `DatasetVariant` decrit une vue/sous-population/agregation.
5. **Multi-target: plusieurs taches mono-target par defaut.** Une tache multi-output reste possible plus tard, mais le benchmark initial indexe chaque target comme `TaskSpec` mono-target.
6. **Pas de baseline canonique imposee.** L'Arena est agnostique. La dataviz permet de choisir une reference interactive, mais le stockage ne suppose pas `PLS-canon`.
7. **Ranking configurable.** Une vue peut classer le CV, un fold, le test externe, le refit, ou une aggregation specifique. Le niveau classe fait partie de la definition versionnee de la vue.
8. **Score versioning explicite.** Tout score porte une version de calcul: metric implementation, politique d'aggregation, filtres, niveau fold/CV/refit et date.
9. **Datasets prives/anonymises supportes.** Si les donnees ne sont pas publiables, l'Arena expose seulement la carte statistique, les scores et les residus autorises par la politique de publication.

## 3. Objectifs

### 3.1 Objectifs produit

- Fournir des benchmarks quantitatifs en ligne pour la communaute NIRS.
- Permettre d'explorer les resultats selon toutes les dimensions: dataset, tache, taille, domaine, split, CV, seed, parametres, operateurs, branche du DAG, modele, strategie de merge/refit, metrique, cout.
- Montrer les effets des operateurs et des parametres, par exemple l'effet de `n_components` sur les scores PLS.
- Rendre visible ce qui marche, ce qui ne marche pas, et dans quelles conditions.
- Permettre l'export des vues et citations pour notebooks, articles et rapports.

### 3.2 Objectifs scientifiques

- Comparer des pipelines sous conditions documentees et, pour les benchmarks officiels, controlees.
- Identifier des patterns par dataset, carte statistique, domaine, taille, tache, instrument, preprocessing, modele et parametres.
- Mesurer la robustesse: variance folds/seeds/splits, instabilite des parametres, taux d'echec, cout.
- Permettre des analyses de residus et de complementarite entre modeles/DAG.

### 3.3 Objectifs techniques

- Fournir un schema d'export/import propre depuis les workspaces nirs4all.
- Normaliser et hasher les dimensions pour dedupliquer: datasets, taches, DAG, operateurs, parametres, y_true quand autorise, residus.
- Permettre des requetes analytiques rapides sans complexifier le format.
- Garder les anciens scores auditables sans mutation silencieuse.

## 4. Non-objectifs

- Heberger les datasets bruts.
- Heberger les modeles entraines ou artefacts fitted.
- Imposer une baseline universelle.
- Imposer une seule definition de leaderboard.
- Refaire le scheduler compute de nirs4all-cluster dans le design initial.
- Remplacer `nirs4all-datasets` ou `nirs4all-methods`.

## 5. Relation avec l'ecosysteme

### 5.1 nirs4all-datasets

`nirs4all-datasets` fournira les datasets de reference versionnes. L'Arena doit anticiper sa v1 avec un mock de `DatasetCard`, puis brancher le vrai catalogue quand il sera stable.

Pour chaque dataset reference, l'Arena stocke:

- identite: `dataset_id`, `dataset_version`, DOI/source, citation, licence;
- acces: URL/API/documentation de telechargement, statut public/restricted/private;
- hashes: `content_hash`, `descriptor_hash`, `dataset_card_hash`;
- carte statistique: n_samples, n_features, signal_type, axis range, missingness, distributions de target, statistiques spectrales, groupes disponibles;
- metadonnees NIRS: instrument, domaine, modalite, source(s), unite d'axe, plage spectrale, resolution si connue;
- splits fournis si le dataset en declare.

Le dataset peut etre prive: l'Arena n'heberge pas les donnees, mais garde la carte d'identite statistique et le lien d'acces si l'utilisateur a les droits.

### 5.2 nirs4all

Le workspace nirs4all actuel utilise:

- `store.sqlite` pour runs, pipelines, chains, predictions, logs, projets;
- `arrays/*.parquet` pour arrays de prediction;
- `artifacts/` pour artefacts fitted.

L'Arena doit reutiliser l'esprit SQLite + Parquet, mais **exclure la retention d'artefacts**. Deux options compatibles:

1. **Extension du workspace nirs4all**: ajouter les tables Arena directement dans `store.sqlite`.
2. **ArenaStore separe**: importer/exporter depuis un workspace nirs4all vers `arena.sqlite + arrays/`.

Le MVP doit commencer par un import propre depuis les workspaces/exports nirs4all, puis pousser les changements utiles dans nirs4all pour produire directement un export Arena propre.

### 5.3 nirs4all-methods

L'Arena reference les methodes, elle ne les stocke pas. Pour chaque operateur venant de `nirs4all-methods` ou d'une dependance externe, on garde:

- bibliotheque/package;
- version;
- entrypoint;
- famille;
- parametres;
- OS/environnement d'execution;
- citation/licence si disponible;
- hash canonique de specification.

## 6. Modele conceptuel

### 6.1 Vue d'ensemble

```text
BenchmarkRelease / UserRunCollection
  -> DatasetCard ou DatasetFingerprint
  -> TaskSpec
  -> DatasetVariant
  -> SplitSpec + SplitInstance
  -> CVSpec + CVInstance
  -> RNGContext
  -> PipelineDAGSpec
  -> RunCondition
  -> ExecutionSummary
  -> ScoreSet + MetricObservation
  -> ResidualSet
```

### 6.2 DatasetCard

Carte d'identite d'un dataset connu, typiquement issu de `nirs4all-datasets`.

Champs:

- `dataset_card_id`;
- `dataset_id`, `dataset_version`, `source_registry`;
- `content_hash`, `descriptor_hash`, `dataset_card_hash`;
- `visibility`: `public`, `restricted`, `private`, `anonymized`;
- `access_policy`: API, documentation, contact, DOI, licence;
- `identity_stats_json`: statistiques descriptives stables;
- `nirs_stats_json`: statistiques spectrales et instrumentales;
- `grouping_metadata_json`: colonnes de groupes, repetitions, lots, instruments;
- `citation_id`.

### 6.3 DatasetFingerprint

Carte anonymisee pour runs utilisateurs ou datasets non partageables.

Elle doit permettre de comparer des regimes de donnees sans exposer les donnees:

- `dataset_fingerprint_hash`;
- `privacy_level`;
- n_samples, n_features, task_type;
- statistiques de X: moyennes/variances resumees, plage spectrale, missingness, outlier scores, SNR estime si disponible;
- statistiques de y: distribution anonymisee, quantiles, nombre de classes, desequilibre;
- statistiques de groupes: nombre de groupes, tailles, repetition rate;
- hash optionnel de sample manifest ou de target si autorise;
- aucune valeur brute obligatoire.

Cette entite sert au moyen terme pour agregation de runs utilisateurs: pas besoin d'un dataset explicite dans `nirs4all-datasets`.

### 6.4 TaskSpec

Une tache correspond a une cible et une definition de prediction. Par defaut, chaque target est une `TaskSpec` separee.

Champs:

- `task_id`;
- `dataset_card_id` ou `dataset_fingerprint_hash`;
- `task_type`;
- `target_name`, `target_unit`, `target_columns`;
- `encoding_json`;
- `target_stats_json`;
- `target_hash` si autorise;
- `task_hash`.

### 6.5 DatasetVariant

Vue d'un dataset pour une tache:

- sous-echantillonnage (`size = 50`, `100`, `all`);
- aggregation (`sample_mean`, `group_mean`, etc.);
- grouping;
- selection de source ou modalite;
- filtrage editorial de lignes;
- codage de target si la variante change la tache effective.

Champs:

- `dataset_variant_id`;
- `dataset_card_id` ou `dataset_fingerprint_hash`;
- `task_id`;
- `variant_spec_json`;
- `sample_manifest_hash` si autorise;
- `dataset_variant_hash`.

### 6.6 SplitSpec et SplitInstance

`SplitSpec` decrit la methode, `SplitInstance` decrit l'instance exacte.

Champs `SplitSpec`:

- `split_method`: random, Kennard-Stone, SPXY, predefined, group split, leave-group-out, etc.;
- `params_json`;
- `group_policy`;
- `stratification_policy`;
- `split_spec_hash`.

Champs `SplitInstance`:

- `split_instance_id`;
- `split_spec_hash`;
- `rng_context_id`;
- `partition_summary_json`;
- `split_indices_hash`;
- indices ou sample ids seulement si autorises.

### 6.7 CVSpec et CVInstance

Meme separation spec/instance:

- methode CV;
- folds/repeats;
- nested CV si besoin;
- relation stricte au train externe (`within_train_only = true`);
- `cv_spec_hash`, `cv_instance_hash`.

### 6.8 RNGContext

Les seeds et RNG sont des conditions experimentales a part entiere.

Champs:

- `rng_context_id`;
- seed utilisateur;
- seeds derivees par framework: Python, NumPy, sklearn, torch, tensorflow, jax;
- etat ou politique RNG si disponible;
- variables deterministes (`PYTHONHASHSEED`, flags CUDA/cuDNN, threads);
- `rng_context_hash`.

### 6.9 PipelineDAGSpec

Le pipeline est un DAG canonique, pas une chaine.

Un DAG contient:

- des **nodes**: preprocessing, augmentation, filtre, feature selector, model, meta-model, merge, stack, bagging, mean, vote, refit, scorer;
- des **edges**: flux de donnees X/y/predictions/residus/metadata entre nodes;
- des **ports**: input/output nommes pour gerer multi-source, multimodal, multi-model;
- des **branches**: chemins paralleles ou conditionnels;
- des **merge nodes**: concat, mean, weighted mean, voting, stacking, bagging, custom reducer;
- des **scopes fit/transform**: train_only, fold_train_only, refit_train_only, inference;
- des **params** par node.

Champs:

- `pipeline_dag_id`;
- `dag_schema_version`;
- `nodes_json`;
- `edges_json`;
- `entry_nodes`, `terminal_nodes`;
- `pipeline_dag_hash`;
- `human_label`;
- `nirs4all_pipeline_manifest_json`.

Invariant: deux syntaxes nirs4all equivalentes doivent produire le meme DAG canonique et le meme `pipeline_dag_hash`.

### 6.10 PipelineNodeSpec

Chaque node est indexable:

- `node_id`;
- `pipeline_dag_hash`;
- `role`;
- `operator_spec_hash`;
- `params_hash`;
- `branch_path`;
- `source_id`;
- `model_family`;
- `fit_scope`.

Cette granularite permet la dataviz par operateur ou parametre: effet de `OSC`, effet de `n_components`, effet d'une branche, effet d'un merge.

### 6.11 OperatorSpec et ParameterSpec

`OperatorSpec`:

- bibliotheque/package;
- version;
- entrypoint;
- role;
- citation/licence;
- `operator_spec_hash`.

`ParameterSpec`:

- `node_id`;
- nom du parametre;
- valeur canonique;
- type;
- `param_value_hash`;
- indicateurs pour dataviz: numeric/categorical, ordinal, sweepable.

Les parametres doivent etre sortis en tables normalisees, pas uniquement caches dans un JSON, pour pouvoir faire des analyses d'effet.

### 6.12 RefitStrategySpec

Le refit est une strategie de DAG:

- `none`;
- `best_fold`;
- `global_best_params_full_train`;
- `per_fold_best`;
- `weighted_average`;
- `stacking_refit`;
- `bagging_refit`;
- `mean_ensemble`;
- autre node terminal du DAG.

Champs:

- `refit_strategy_hash`;
- `selection_scope`;
- `train_scope`;
- `params_json`.

### 6.13 RunCondition

Identite naturelle d'une condition experimentale:

- `benchmark_release_id` ou `user_run_collection_id`;
- `dataset_variant_hash`;
- `split_instance_hash`;
- `cv_instance_hash`;
- `rng_context_hash`;
- `pipeline_dag_hash`;
- `refit_strategy_hash`;
- `run_condition_hash`.

Les colonnes derivees utiles pour requete (modele principal, presence de SNV, valeur de `n_components`, etc.) peuvent etre materialisees, mais la source canonique reste le DAG.

### 6.14 ExecutionSummary

Resume d'une execution concrete, sans artefact fitted:

- `execution_id`;
- `run_condition_hash`;
- `producer_capsule`: python, R, studio, studio-lite, cluster;
- `nirs4all_version`, `dag_ml_version` si applicable;
- OS, Python/R version, dependances majeures;
- hardware resume;
- temps/memoire;
- statut: ok/failed/cancelled;
- failure code/message tronque;
- `execution_hash`.

### 6.15 ScoreComputationSpec et ScoreSet

Le versioning de score est explicite.

`ScoreComputationSpec`:

- `score_version`;
- metric implementation;
- niveau score: fold, CV, refit, test, custom view;
- politique d'aggregation;
- filtres;
- direction;
- normalisation eventuelle;
- `score_computation_hash`.

`ScoreSet`:

- `score_set_id`;
- `execution_id`;
- `score_computation_hash`;
- `scope`: fold, cv, refit, test, view;
- `created_at`;
- `supersedes_score_set_id` si recalcul;
- `validity_status`.

Un recalcul de score ajoute un nouveau `ScoreSet`; il ne remplace pas silencieusement l'ancien.

### 6.16 MetricObservation

Format long:

- `score_set_id`;
- `metric_name`;
- `metric_value`;
- `metric_unit`;
- `direction`;
- `fold_id`;
- `partition`;
- `aggregation_level`;
- `n_samples`;
- `coverage`.

### 6.17 ResidualSet

L'Arena conserve les residus, pas les artefacts.

Parquet minimal:

- `residual_set_id`;
- `execution_id`;
- `score_set_id`;
- `fold_id`;
- `partition`;
- `sample_pseudo_id`;
- `group_pseudo_id` si autorise;
- `residual`;
- `y_pred` optionnel;
- `y_true` optionnel;
- `weight` optionnel.

Politique:

- pour dataset public: `y_true`, `y_pred`, residus peuvent etre publies si licence compatible;
- pour dataset restricted/private: publier par defaut scores agreges et residus seulement si la politique le permet;
- les ids sont pseudonymises.

### 6.18 IngestionBatch

Lot transactionnel:

- `ingestion_batch_id`;
- source workspace/export nirs4all;
- `input_export_hash`;
- statut staging/committed/rejected;
- compteurs de lignes;
- erreurs de validation;
- `clean_report_json`.

L'idempotence utilise `(input_export_hash, target_collection, schema_version)`.

### 6.19 ViewDefinition

Une vue publiee est versionnee:

- `view_id`;
- leaderboard, matrix, pp-effect, param-effect, DAG explorer, residual explorer;
- filtres;
- niveau score;
- aggregation;
- date de materialisation;
- `view_definition_hash`.

## 7. Stockage physique

### 7.1 Format cible

Le format cible prolonge le workspace nirs4all:

```text
workspace-or-arena/
  store.sqlite
  arrays/
    residuals_<partition>.parquet
    optional_predictions_<partition>.parquet
  exports/
    arena_export_<hash>.zip
```

Pas de `artifacts/` dans l'Arena. Si un workspace nirs4all contient des artefacts pour ses besoins internes, l'export Arena les ignore.

### 7.2 SQLite

SQLite stocke les dimensions et faits:

- dataset cards/fingerprints/tasks/variants;
- split/CV/RNG;
- pipeline DAGs/nodes/edges/operators/params;
- run conditions;
- execution summaries;
- score computation specs;
- score sets;
- metric observations;
- ingestion batches;
- view definitions.

SQLite convient car:

- il est deja dans nirs4all;
- il est portable;
- il gere bien des millions de lignes indexees si le schema est normalise;
- il evite la lenteur et la complexite observees avec DuckDB dans ce contexte.

### 7.3 Parquet

Parquet stocke uniquement les arrays de resultats:

- residus;
- y_pred/y_true optionnels;
- poids;
- ids pseudonymises;
- metadata minimale de fold/partition.

Les arrays doivent etre dedupliques autant que possible:

- `target_vector_hash` quand y_true est stockable;
- dictionnaires de sample ids;
- dictionnaires de fold ids;
- compression Zstd;
- compactage periodique.

### 7.4 Deduplication

Tables de dedup prioritaires:

- `operator_specs`;
- `parameter_values`;
- `pipeline_dag_specs`;
- `dataset_cards`;
- `dataset_fingerprints`;
- `task_specs`;
- `split_instances`;
- `cv_instances`;
- `rng_contexts`;
- `score_computation_specs`;
- `target_vectors` seulement si autorise;
- `residual_sets`.

La dedup sert la performance mais aussi la dataviz: un operateur ou parametre identique doit etre requetable partout.

### 7.5 Implications pour le workspace nirs4all actuel

Le schema actuel `store.sqlite + arrays/*.parquet` est une bonne base, mais il doit evoluer pour l'Arena.

Mapping initial:

| Workspace actuel | Usage Arena | Evolution necessaire |
|---|---|---|
| `runs` | source de batch/session | ajouter/exporter release, collection, RNG, score version |
| `pipelines` | configuration pipeline | remplacer/augmenter par `PipelineDAGSpec` canonique |
| `chains` | chemin lineaire ou branche simple | generaliser en `pipeline_nodes` + `pipeline_edges` |
| `predictions` | metadonnees fold/partition/scores | separer score metadata, residual metadata et scope fold/CV/refit |
| `arrays/*.parquet` | y_true/y_pred actuels | produire `ResidualSet` dedup, y_true/y_pred optionnels |
| `artifacts` | cache fitted interne | ignore par l'export Arena |
| `logs` | diagnostic local | garder seulement resume d'erreur/cout si utile |

Tables nouvelles ou a materialiser:

- `dataset_cards`, `dataset_fingerprints`, `task_specs`, `dataset_variants`;
- `pipeline_dags`, `pipeline_nodes`, `pipeline_edges`;
- `operator_specs`, `parameter_values`;
- `split_specs`, `split_instances`, `cv_specs`, `cv_instances`;
- `rng_contexts`;
- `score_computation_specs`, `score_sets`, `metric_observations`;
- `residual_sets`;
- `ingestion_batches`, `view_definitions`.

Le point cle est de ne pas forcer nirs4all a abandonner son stockage d'artefacts pour ses besoins de replay local. L'Arena definit seulement un **profil d'export sans artefacts**.

## 8. Ingestion et clean

### 8.1 Sources

- workspace nirs4all local;
- export nirs4all dedie Arena;
- runs Studio/Studio Lite via export;
- batch cluster nirs4all;
- imports historiques convertis en format nirs4all si possible.

### 8.2 Pipeline d'ingestion

```text
workspace/export
  -> detect schema
  -> clean legacy fields
  -> validate dataset card/fingerprint
  -> canonicalize pipeline DAG
  -> extract operator/param tables
  -> compute hashes
  -> validate split/CV/RNG
  -> extract scores and residuals
  -> drop fitted artifacts
  -> write staging
  -> commit SQLite + Parquet
  -> refresh views
```

### 8.3 Clean attendu

Le clean d'un workspace/export doit:

- supprimer toute reference a artefacts fitted non conserves;
- normaliser les noms de modeles, operateurs, branches et sources;
- extraire les parametres de JSON vers tables normalisees;
- verifier les doublons via hashes;
- typer les echelles: fold/CV/refit/test;
- recalculer ou tagger les scores selon `ScoreComputationSpec`;
- produire un rapport lisible.

### 8.4 Validation minimale

- dataset card ou fingerprint present;
- task mono-target explicite;
- split/CV compatibles avec le nombre d'echantillons;
- RNGContext present;
- PipelineDAG canonique valide;
- pas de leakage detecte dans split/CV;
- score version present;
- residus alignes avec fold/partition;
- politique de publication compatible avec les arrays exportes.

## 9. Dataviz et web app

### 9.1 MVP

1. **Explorer de runs**: table filtrable par dataset, task, DAG, node, parametre, split, CV, seed.
2. **Leaderboard configurable**: choix du niveau score, metrique, aggregation, filtres.
3. **Matrice pipeline x dataset/task**: scores, coverage, echec.
4. **Detail run**: DAG, nodes, params, folds, scores, residus.

### 9.2 Vues prioritaires

- Effet operateur: ex. impact de `OSC` quand present dans un DAG.
- Effet parametre: ex. `n_components` PLS vs score.
- Effet split/CV/seed.
- Comparaison de deux DAGs.
- Residual explorer.
- Robustesse et taux d'echec.
- Cout performance.

### 9.3 Playground

Le playground doit permettre:

- selection interactive d'une reference, sans baseline canonique imposee;
- groupby arbitraire: dataset card, fingerprint stats, node role, parametre, merge strategy;
- extraction notebook-ready;
- sauvegarde de vues versionnees.

## 10. Versioning, invalidation et scoring

### 10.1 Score versioning

Un score n'est jamais seulement un nombre. Il est toujours attache a:

- une metrique;
- une implementation;
- un niveau fold/CV/refit/test;
- une aggregation;
- une politique de filtres;
- une date et une version.

Si une metrique est corrigee, l'Arena ajoute une nouvelle version de score. Les anciennes restent auditables et marquees comme superseded.

### 10.2 Invalidation

Les invalidations sont explicites:

- leakage detecte;
- bug metric;
- bug ingestion;
- dataset retire;
- politique de publication changee;
- run corrompu.

Une invalidation ne supprime pas les lignes; elle change seulement les vues valides.

### 10.3 Releases

- **Patch**: bug affichage ou nouvelle vue, pas de rerun.
- **Minor**: nouveaux datasets/methodes/vues, pas de changement de protocole obligatoire.
- **Major**: nouvelle grille ou nouvelle politique de score officielle.

Une release peut publier plusieurs leaderboards, chacun avec sa `ViewDefinition`.

## 11. Gouvernance et confidentialite

### 11.1 Public, restricted, private

Tout ce qui est publie est visible. Pour les datasets prives, la publication doit se limiter aux informations autorisees:

- carte statistique;
- scores agreges;
- residus seulement si autorises;
- pas de raw X;
- pas de raw y sauf politique explicite.

### 11.2 Citation

Le pipeline contient l'integralite des operateurs et parametres; l'Arena doit conserver les citations des bibliotheques et methodes quand elles existent.

Les exports de vues doivent produire une liste de citations:

- dataset ou dataset card;
- methode/operator source;
- nirs4all version;
- protocole/view definition.

## 12. Roadmap parallele

### Vue d'ensemble

Les chantiers sont concus pour avancer en parallele. Les dependances bloquantes sont limitees a deux contrats communs: `PipelineDAGSpec` et `ArenaStore schema`.

| Stream | Objectif | Peut demarrer quand | Livrable |
|---|---|---|---|
| A. Schema Arena | Tables SQLite + schemas Parquet | maintenant | `arena_schema.sql`, schemas arrays |
| B. Pipeline DAG | Canonicalisation DAG nirs4all/dag-ml | maintenant | `PipelineDAGSpec` + hash |
| C. Dataset cards | Mock puis integration `nirs4all-datasets` v1 | maintenant avec mock | `DatasetCard`, `DatasetFingerprint` |
| D. Ingestion workspace | Import/clean depuis workspace nirs4all | apres A partiel | `n4a-arena ingest-workspace` |
| E. Scores versionnes | `ScoreComputationSpec`, recalcul, supersede | apres A partiel | score versioning |
| F. Residual store | Parquet residus + politiques publication | apres A partiel | `ResidualSet` |
| G. Dataviz MVP | UI/exports sur donnees fixtures | avec fixtures A/C/B | explorer + leaderboard configurable |
| H. nirs4all export | Evolution lib pour export Arena propre | apres B/D prototypes | `nirs4all export-arena` |
| I. Tests/fixtures | Jeu 2 datasets x DAGs x scores | maintenant | fixtures de validation |

### Phase 0 - Contrats et fixtures

Taches paralleles:

- A1. Ecrire le schema SQLite Arena minimal.
- B1. Definir JSON canonical `PipelineDAGSpec`.
- C1. Definir `DatasetCard` mock compatible future v1 datasets.
- I1. Creer fixture avec 2 datasets, 2 tasks mono-target, 3 DAGs dont un branche/merge.
- E1. Definir `ScoreComputationSpec`.

Sortie: fixtures chargeables et hashes stables.

### Phase 1 - Store local

Taches paralleles:

- A2. Implementer `ArenaStore` SQLite + Parquet.
- D1. Importer workspace nirs4all existant sans artefacts.
- F1. Ecrire/relire `ResidualSet` Parquet.
- E2. Ingerer scores fold/CV/refit avec version explicite.
- B2. Extraire nodes/operators/params depuis pipeline nirs4all.

Sortie: `n4a-arena ingest-workspace <workspace>` et requetes locales.

### Phase 2 - Clean et dedup

Taches paralleles:

- D2. Rapport de clean: champs legacy, artefacts ignores, scores classes.
- A3. Tables dedup operators/params/DAG/dataset/task/RNG.
- F2. Dedup y_true/residus quand autorise.
- E3. Recalcul ou supersede de scores.
- C2. Support `DatasetFingerprint` pour runs utilisateurs anonymises.

Sortie: ingestion idempotente, compacte et auditable.

### Phase 3 - Dataviz MVP

Taches paralleles:

- G1. Explorer runs/conditions.
- G2. Leaderboard configurable sans baseline canonique.
- G3. Matrice DAG x dataset/task.
- G4. Effet operateur/parametre.
- G5. Detail DAG + folds + residus.

Sortie: site/app interne exploitable sur fixtures et premiers workspaces.

### Phase 4 - Integration nirs4all

Taches paralleles:

- H1. Ajouter export Arena propre dans nirs4all.
- H2. Ajouter emission `PipelineDAGSpec` depuis nirs4all/dag-ml.
- H3. Ajouter `RNGContext` complet dans les runs.
- H4. Ajouter hooks Studio/Studio Lite vers export Arena.
- D3. Valider import depuis exports produits par toutes les capsules.

Sortie: producteur unifie nirs4all.

### Phase 5 - Public benchmark

Taches paralleles:

- C3. Brancher `nirs4all-datasets` v1.
- G6. Exports publics avec politiques restricted/private.
- E4. Definir premieres `ViewDefinition` publiques.
- I2. Tests de non-regression sur hashes/scores.
- Documentation contribution et citation.

Sortie: premiere Arena publique utilisable.

## 13. Questions residuelles

- Quel format exact de `PipelineDAGSpec` sera partage avec dag-ml ?
- Quels residus peut-on publier pour chaque niveau de confidentialite ?
- Quelle taille limite pour conserver les residus de grands runs utilisateurs ?
- Faut-il materialiser certaines colonnes derivees du DAG pour accelerer les vues, ou les calculer a la demande ?

## 14. Decisions a appliquer au prochain dev

1. Partir de SQLite + Parquet, pas DuckDB.
2. Ne pas stocker les artefacts fitted.
3. Faire de `PipelineDAGSpec` le coeur du design.
4. Indexer les operateurs et parametres pour analyser leurs effets.
5. Supporter les datasets anonymises via `DatasetFingerprint`.
6. Rendre le score versionne et auditable.
7. Construire d'abord l'ingestion/clean depuis workspace nirs4all.
