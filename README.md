# PYSPARK PROJECT 
## Chosen dataset: **imdb**
## Participants:
    - Ostap Shcherbii     [CS-413]
    - Mykytyn Sofiia      [CS-413]
    - Molochii Viktoriia  [CS-415]
    - Yurii Yano          [CS-413]
    - Iryna Koval         [CS-413]
    - Vasyl Shponarskyi   [CS-413]

# Steps to try our program:

## 1) Build image

```python
docker build -t spark-image:1.0 .
```

## 2) To run in debug-mode:
```python
docker run -p 80:3000 --rm -v 'PATH_TO_A_FOLDER_WHERE_DOCKERFILE_IS_LOCATED:/app' --name container-spark-dev-mode spark-image:1.0
```

### After first run, "datasets" folder will be created. Please, insert there all needed dfs from [LINK](https://datasets.imdbws.com/):
    - name.basics.tsv
    - title.akas.tsv
    - title.basics.tsv
    - title.crew.tsv
    - title.episode.tsv
    - title.principals.tsv
    - title.ratings.tsv

## 3) Repeat previous step
