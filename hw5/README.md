# ДЗ №5. Kubernetes

Команды выполняются в PowerShell из корня репозитория. Нужны Docker с Linux-контейнерами, kind v0.31.0, kubectl, Python 3.11 и исходный `it_vacancies_2024.csv` из ДЗ №2. Для повторного прогона нужен свежий кластер `hw5`.

## Кластер и образы

```powershell
python -m pip install boto3==1.43.83 PyYAML==6.0.2
kind create cluster --name hw5 --image kindest/node:v1.35.0 --wait 120s
docker build --provenance=false -f hw5/Dockerfile -t hw5-pairs:v1 .
$rustImage = 'rustfs/rustfs@sha256:1803faef57627e2d9c2e7d89d655d712ddded5389040054987163043fecb6a3c'
docker pull --platform linux/amd64 $rustImage
docker tag $rustImage hw5-rustfs:hw2
docker save --platform linux/amd64 -o "$env:TEMP\hw5-images-amd64.tar" hw5-pairs:v1 hw5-rustfs:hw2
cmd.exe /d /c 'docker exec -i hw5-control-plane ctr -n k8s.io images import --platform linux/amd64 - < "%TEMP%\hw5-images-amd64.tar"'
```

## Расчёт и эксперименты

Для переноса из запущенного RustFS ДЗ №2 задайте `SOURCE_ACCESS_KEY` и `SOURCE_SECRET_KEY` в окружении. В хранилище должен быть объект `s3://vacancies/dataset/it_vacancies_2024.csv`. Подставьте адрес исходного сервера.

```powershell
python hw5/scripts/run.py data --source-endpoint http://127.0.0.1:19051
```

Для запуска с локальной копией исходного CSV используйте вместо этой команды:

```powershell
python hw5/scripts/run.py data --dataset ../eng_dan/it_vacancies_2024.csv
```

Скрипт создаёт Secret автоматически. Команды для создания Secret со своими ключами из переменных окружения:

```powershell
kubectl --context kind-hw5 apply -f hw5/manifests/rustfs.yaml
kubectl --context kind-hw5 -n hw5 create secret generic s3-credentials `
  --from-literal="AWS_ACCESS_KEY_ID=$env:AWS_ACCESS_KEY_ID" `
  --from-literal="AWS_SECRET_ACCESS_KEY=$env:AWS_SECRET_ACCESS_KEY"
```

Эксперименты с CPU, вытеснением и квотой и сверка топ-10:

```powershell
python hw5/scripts/run.py resources
python hw5/scripts/pipeline.py compare hw5/results/reference.csv hw5/results/parallel.csv
```
