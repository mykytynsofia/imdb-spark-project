gsutil cp ./title_ratings_job.py gs://jobs-codebase/title_ratings_job.py
gsutil cp ./title_ratings_preprocessing.py gs://jobs-codebase/title_ratings_preprocessing.py
cd terraform && terraform destroy -auto-approve && terraform apply -auto-approve