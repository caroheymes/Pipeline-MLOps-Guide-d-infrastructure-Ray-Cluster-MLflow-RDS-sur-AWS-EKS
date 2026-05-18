
# Pipeline MLOps : Guide d'infrastructure Ray Cluster, MLflow & RDS sur AWS EKS

## Principe de l'Architecture

Pour comprendre l'organisation de cette documentation, on peut imaginer le déploiement comme un chantier de construction :

* **Les Murs (Le Cluster) :** On commence par bâtir les fondations et la structure solide de notre environnement en déployant le cluster **AWS EKS**.
* **Les Badges (La Sécurité) :** On configure le contrôle d'accès en créant des badges de sécurité nominatifs grâce au pont **OIDC** et aux rôles **IAM (IRSA)**.
* **Les Ouvriers (Les Outils MLOps) :** On fait entrer **MLflow** et **Ray** sur le chantier. On leur distribue leurs badges pour qu'ils puissent accéder aux ressources (base RDS, stockage) et travailler en toute sécurité.

```text
▲ [MONDE EXTÉRIEUR / UTILISATEUR]
│
▼ (Accès via `kubectl` / Envoi de Jobs)
┌─────────────────────────────────────────────────────────────────────────────┐
│ AWS CLOUD (VPC)                                                             │
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │ CLUSTER AWS EKS (Les Murs 🏗️)                                          │  │
│  │                                                                       │  │
│  │   [Fournisseur d'identité OIDC] ◄─── (Vérification des tokens)        │  │
│  │          ▲                                                            │  │
│  │          │ Échange de clés (Confiance)                                │  │
│  │          ▼                                                            │  │
│  │   [Rôles IAM / IRSA] (Les Badges 🪪) ──────────────────────────────┐  │  │
│  │                                                                    │  │  │
│  │   ┌──────────────────────────────────────────────────────────────┐ │  │  │
│  │   │ ESPACE APPLICATIF (Les Ouvriers 👷)                          │ │  │  │
│  │   │                                                              │ │  │  │
│  │   │  ┌──────────────────┐           ┌────────────────────────┐   │ │  │  │
│  │   │  │   Pod MLflow     │           │   Kuberay Operator     │   │ │  │  │
│  │   │  │  (Tracking UI)   │           └───────────┬────────────┘   │ │  │  │
│  │   │  └────────┬─────────┘                       │ Déploie        │ │  │  │
│  │   │           │                                 ▼                │ │  │  │
│  │   │           │                     ┌────────────────────────┐   │ │  │  │
│  │   │           │                     │  Ray Cluster (Head)    │   │ │  │  │
│  │   │           │                     └───────────┬────────────┘   │ │  │  │
│  │   │           │                                 │ Autoscale      │ │  │  │
│  │   │           │                                 ▼                │ │  │  │
│  │   │           │                     ┌────────────────────────┐   │ │  │  │
│  │   │           │                     │  Ray Workers (Pods)    │   │ │  │  │
│  │   │           │                     └────────────────────────┘   │ │  │  │
│  │   └───────────┼──────────────────────────────────────────────────┘ │  │  │
│  └───────────────┼────────────────────────────────────────────────────┼──┘  │
│                  │                                                    │     │
│                  │ (Stockage des Runs/Métriques)                      │     │
│                  ▼                                                    │     │
│       ┌──────────────────────┐                                        │     │
│       │  Base AWS RDS (DB)   │ ◄──────────────────────────────────────┤     │
│       │  (PostgreSQL)        │    (Autorise l'accès via Sec. Group)   │     │
│       └──────────────────────┘                                        │     │
│                                                                       │     │
│                  ▲ (Stockage des Artefacts / Modèles)                 │     │
│                  │                                                    │     │
│       ┌──────────┴───────────┐                                        │     │
│       │   Bucket AWS S3      │ ◄──────────────────────────────────────┘     │
│       │   (Artifact Store)   │       (Autorise les accès S3 Read/Write)     │
│       └──────────────────────┘                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

# Sommaire

## Phase 1 : Construction des murs (Cluster EKS)
- [1. Setup](#1-setup)
- [2. Creation du cluster dans ray-ml-cluster.yaml](#2-creation-du-cluster-dans-ray-ml-clusteryaml)

## Phase 2 : Création des badges de sécurité (IRSA / OIDC / Réseau)
- [3. Configuration IAM Roles for Service Account (IRSA)](#3-configuration-iam-roles-for-service-account-irsa)
  - [Permissions sécurisées for Service Account (IRSA) avec eksctl](#permissions-sécurisées-for-service-account-irsa-avec-eksctl)
    - [3.1. Activer le pont OIDC](#31-activer-le-pont-oidc)
    - [3.2. Créer le compte de service (Service Account)](#32-créer-le-compte-de-service-service-account)
    - [3.3. Récupérer l'ID du réseau (VPC) et les subnets](#33-récupérer-lid-du-réseau-vpc-et-les-subnets)
    - [3.4. Groupe de sécurité](#34-groupe-de-sécurité)
    - [3.5. Autoriser le trafic PostgreSQL](#35-autoriser-le-trafic-postgresql)
    - [3.6. Créer le groupe de sous-réseaux RDS](#36-créer-le-groupe-de-sous-réseaux-rds)
    - [3.7. Gestion des secrets (mot de passe pur la base de données)](#37-gestion-des-secrets-mot-de-passe-pur-la-base-de-données)
- [4. Création de la base RDS](#4-création-de-la-base-rds)
- [5. Gérer l'accès de la DB au secret dans Kubernetes](#5-gérer-laccès-de-la-db-au-secret-dans-kubernetes)

## Phase 3 : Entrée des ouvriers et exécution des travaux (MLflow / Ray)
- [6. Déploiement MLflow](#6-déploiement-mlflow)
- [7. Ray Cluster](#7-ray-cluster)
  - [7.1. Installer Kuberay](#71-installer-kuberay)
  - [7.2. Manifest RAY `ray-cluster.yaml`](#72-manifest-ray-ray-clusteryaml)
- [8. Scaling](#8-scaling)
  - [8.1. Avec un rolling update suite à une modif de machine](#81-avec-un-rolling-update-suite-à-une-modif-de-machine)
  - [8.2. Détruire et recréer le groupe](#82-détruire-et-recréer-le-groupe)
- [9. Entraîner un modèle](#9-entraîner-un-modèle)

## Nettoyage du chantier
- [10. Suppression des ressources](#10-suppression-des-ressources)

---
# Phase 1 : Construction des murs (Cluster EKS)

## 1. Setup
```powershell
# Configuration des identifiants AWS
aws configure

# intall eksctl
choco install eksctl

# profil admin par défaut 
$PROFILE = <PROFIL_AVEC_DROITS_ADMIN>
$ACCOUNT_ID = "..."
$REGION = "..."

# 2. se  loguer 
aws sso login --profile $PROFILE

# 3. Verif admin
aws sts get-caller-identity --profile $PROFILE
```
---
## 2. Création du cluster dans ray-ml-cluster.yaml
```yaml
apiVersion: eksctl.io/v1alpha5
kind: ClusterConfig

metadata:
  name: ray-ml-cluster
  region: eu-west-1

managedNodeGroups:
  # 1. Le groupe économique (pour faire tourner la base)
  - name: standard-nodes
    instanceType: t3.large
    desiredCapacity: 2
    minSize: 1
    maxSize: 3
    # spot: true # Économique mais désactivé car ça a foutu la m...

  # 2. Le groupe GPU (qui démarre à 0 et monte si besoin)
  - name: gpu-nodes
    instanceType: g4dn.xlarge # Contient 1 GPU NVIDIA T4 (Le moins cher des GPUs AWS)
    desiredCapacity: 0        # <--- Commencer à 0 machine pour ne rien payer !
    minSize: 0
    maxSize: 2
    # spot: true                # <--- Version Spot (environ 0,16 $ / heure au lieu de 0,52 $)
    labels:
      k8s.amazonaws.com/accelerator: nvidia-t4
```

```powershell
eksctl create cluster -f ray-ml-cluster.yaml --profile $PROFILE
#verif
kubectl config get-contexts
kubectl get nodes -o wide          #santé des nœuds ok
kubectl get pods -n kube-system # composants système d'AWS ok
$CLUSTER_NAME = ((aws eks list-clusters --region $REGION --profile $PROFILE --output json) | ConvertFrom-Json).clusters[0]

```
<p align="right"><a href="#sommaire">▲ Retour au sommaire</a></p>
---
# Phase 2 : Création des badges de sécurité (IRSA / OIDC / Réseau)
## 3. Configuration IAM Roles for Service Account (IRSA)

### = Permissions sécurisées for Service Account (IRSA) avec eksctl

### 3.1. Activer le pont OIDC
OIDC est l'acronyme de *OpenID Connect*. Dans le contexte du projet c'est une couche d'authentification basée sur le protocole OAuth 2.0.

Dans l'infrastructure AWS EKS, le pont OIDC sert de "passerelle de confiance" : il permet à au cluster Kubernetes de prouver de manière sécurisée à AWS IAM que tel ou tel composant (comme Ray ou MLflow) a le droit de demander un badge d'accès (un rôle IRSA - IAM Role for service account).

```powershell
# activer la passerelle de confiance entre kubernetes et AWS
eksctl utils associate-iam-oidc-provider `
  --cluster $CLUSTER_NAME `
  --region $REGION `
  --approve `
  --profile $PROFILE
```
### 3.2. Créer le compte de service (Service Account)
```powershell
eksctl create iamserviceaccount `
  --name mlflow-s3-access `
  --namespace default `
  --cluster $CLUSTER_NAME `
  --attach-policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess `
  --approve `
  --override-existing-serviceaccounts `
  --region $REGION `
  --profile $PROFILE
```
### 3.3. Récupérer l'ID du réseau (VPC) et les subnets

```powershell
aws eks describe-cluster `
  --name $CLUSTER_NAME `
  --region $REGION `
  --profile $PROFILE `
  --query "cluster.resourcesVpcConfig.{VpcId:vpcId, Subnets:subnetIds}" `
  --output table

  #on stocke dans une variable VPC_ID
$VPC_ID = ((aws eks describe-cluster `
--name $CLUSTER_NAME `
--region $REGION `
--profile $PROFILE `
--query "cluster.resourcesVpcConfig.{VpcId:vpcId, Subnets:subnetIds}" `
--output json) | ConvertFrom-Json).VpcId

# variable $SUBNETS
$SUBNETS = ((aws eks describe-cluster `
--name $CLUSTER_NAME `
--region $REGION `
--profile $PROFILE `
--query "cluster.resourcesVpcConfig" `
--output json) | ConvertFrom-Json).subnetIds
```
### 3.4. Groupe de sécurité
```powershell

$SG_ID = aws ec2 create-security-group `
  --group-name mlflow-rds-sg `
  --description "SG for MLflow RDS" `
  --vpc-id $VPC_ID `
  --region $REGION `
  --profile $PROFILE `
  --query "GroupId" `
  --output text
```
### 3.5. Autoriser le trafic PostgreSQL
= accepter les connexions sur le port 5432 (PostgreSQL), uniquement si elles proviennent de l'intérieur de ton réseau

```powershell
# On cherche le Client IP Routing du VPC qui correspond au CidrBlock
$CIDR = ((aws ec2 describe-vpcs `
    --vpc-ids $VPC_ID ` `
    --output json `
    --region $REGION `
    --profile $PROFILE) | ConvertFrom-Json).Vpcs.CidrBlock


aws ec2 authorize-security-group-ingress `
  --group-id $SG_ID `
  --protocol tcp `
  --port 5432 `
  --cidr $CIDR `
  --region $REGION `
  --profile $PROFILE
```

### 3.6. Créer le groupe de sous-réseaux RDS
AWS a besoin qu'on "regroupe" les sous-réseaux EKS dans un dossier spécial pour RDS. C'est ce qui garantit que la base sera déployée exactement sur les mêmes routes que tes nœuds
```powershell
aws rds create-db-subnet-group `
  --db-subnet-group-name "mlflow-db-subnet-group" `
  --db-subnet-group-description "Subnet group for MLflow RDS" `
  --subnet-ids $SUBNETS `
  --region $REGION `
  --profile $PROFILE
```
### 3.7. Gestion des secrets (mot de passe pur la base de données)
On crée le secret dans aws secrets manager > autre type de secrets
Clé : <SECRET_NAME> | Valeur : 
Clé : <DB_PASSWORD> | Valeur :xxx
Nommer le secret `par-exemple-test-mon-mlflow-db-password`

```powershell
récup le mot de passe
$DB_PASSWORD = (((aws secretsmanager get-secret-value `
   --secret-id "mon-mlflow-db-password" `
   --output json `
   --region $REGION `
   --profile $PROFILE) | ConvertFrom-Json).SecretString | ConvertFrom-JSON).password
```
<p align="right"><a href="#sommaire">▲ Retour au sommaire</a></p>
---

## 4. Création de la base RDS
```powershell
aws rds create-db-instance `
  --db-instance-identifier mlflow-postgres-prod `
  --db-instance-class db.t3.micro `
  --engine postgres `
  --allocated-storage 20 `
  --master-username mlflow_user `
  --master-user-password $DB_PASSWORD `
  --vpc-security-group-ids $SG_ID `
  --db-subnet-group-name mlflow-db-subnet-group `
  --no-publicly-accessible `
  --region $REGION `
  --profile $PROFILE

# Statut de la création 
aws rds describe-db-instances `
  --db-instance-identifier mlflow-postgres-prod `
  --query "DBInstances[0].DBInstanceStatus" `
  --output text `
  --region $REGION `
  --profile $PROFILE

# récup endpoint (par itérations !)
$DB_ENDPOINT = ((aws rds describe-db-instances `
   --region $REGION `
   --profile $PROFILE `
   --output json) | ConvertFrom-Json).DBInstances[0].Endpoint.Address
# récup le nom de la base
((aws rds describe-db-instances `
   --region $REGION `
   --profile $PROFILE `
   --output json) | ConvertFrom-Json).DBInstances.DBName
# si rien n'est renvoyé alors DBName est "postgres"

# Utilisateur
((aws rds describe-db-instances `
   --region $REGION `
   --profile $PROFILE `
   --output json) | ConvertFrom-Json).DBInstances.MasterUsername

```
<p align="right"><a href="#sommaire">▲ Retour au sommaire</a></p>
---

## 5. Gérer l'accès de la DB au secret dans Kubernetes
Dans Kubernetes **`ESO - External Secrets Operator`** permet d'accéder au secret `mon-mlflow-db-password ` stocké dans le aws secrets manager.

```powershell
#Installation du SecretStore via Helm

#le cas échéant
helm uninstall external-secrets --namespace external-secrets
helm repo add external-secrets https://charts.external-secrets.io
helm repo update
helm install external-secrets external-secrets/external-secrets `
  --namespace external-secrets `
  --create-namespace `
  --set installCRDs=true

kubectl get pods -n external-secrets
kubectl get crd | select-string "external-secrets"
```

1. On doit spécifier l'authentification pour se connecter au secret manager
2. On doit injecter ce secret dans Kubernetes

1. Spécifier l'authentification avec SecretStore
`patch-store.yaml`
```yaml
apiVersion: external-secrets.io/v1
kind: SecretStore
metadata:
  name: aws-secret-store
  namespace: default
spec:
  provider:
    aws:
      service: SecretsManager
      region: $REGION
      auth:
        jwt:
          serviceAccountRef:
            name: external-secrets-service-account
```
```powershell
kubectl apply -f patch-store.yaml
```


2. Injecter le secret `mon-mlflow-db-password ` dans Kubernetes

nb : La variable $DB_PASSWORD créée dans le shell a servi uniquement pour la commande `aws rds create-db-instance`

`external-secret.yaml`
```yaml
apiVersion: external-secrets.io/v1 # gaffe à la version de l'API
kind: ExternalSecret
metadata:
  name: mlflow-db-secret
  namespace: default
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: aws-secret-store
    kind: SecretStore
  target:
    name: mlflow-db-secret # Le nom du secret qui sera créé dans Kubernetes
    creationPolicy: Owner
  data:
    - secretKey: password # Le nom de la clé dans K8s
      remoteRef:
        key: mon-mlflow-db-password # Le nom exact du secret dans AWS
        property: password # On cible uniquement la clé "password" 
```
```Powershell
eksctl create iamserviceaccount `
  --name eso-aws-sa `
  --namespace default `
  --cluster $CLUSTER_NAME `
  --attach-policy-arn arn:aws:iam::aws:policy/SecretsManagerReadWrite `
  --approve `
  --region $REGION `
  --profile $PROFILE
  

kubectl apply -f external-secret.yaml
kubectl get externalsecrets`
```
<p align="right"><a href="#sommaire">▲ Retour au sommaire</a></p>
---
## Phase 3 : Entrée des ouvriers et exécution des travaux (MLflow / Ray)

## 6. Déploiement MLflow


```PowerShell
# A toutes fins utiles  
#lister les services accounts
kubectl get sa

powershell
# stockage des artefacts (S3) -  création du bucket
aws s3 mb s3://mlflow-artifacts-ray-ml-cluster --region eu-west-1
aws s3 ls

# Service account
eksctl create iamserviceaccount `
  --name mlflow-s3-access  `
  --namespace default `
  --cluster $CLUSTER_NAME `
  --attach-policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess `
  --approve `
  --override-existing-serviceaccounts `
  --region $REGION `
  --profile $PROFILE

# Création du ServiceAccount K8s autorisé à lire les secrets AWS
eksctl create iamserviceaccount `
  --name eso-aws-sa `
  --namespace default `
  --cluster $CLUSTER_NAME `
  --attach-policy-arn arn:aws:iam::aws:policy/SecretsManagerReadWrite `
  --approve `
  --region $REGION `
  --profile $PROFILE

# On déploie l'application et le service
kubectl apply -f mlflow-values.yaml

# On surveille le démarrage du pod
kubectl get pods -w

# accès à l'interface
kubectl port-forward svc/mlflow-service 5000:5000
```
<p align="right"><a href="#sommaire">▲ Retour au sommaire</a></p>
---

## 7. Ray Cluster

### Builder et Pousser l'image sur AWS Elastic Container Registry

```PowerShell
# Créer le dépôt ECR (à ne faire qu'une seule fois)
$REPO_NAME="ray-training"
aws ecr create-repository `
  --repository-name $REPO_NAME `
  --region $REGION `
  --profile $PROFILE

# virer les éventuels credentials gcloud avec 
$env:USERPROFILE\.docker\config.json
code $env:USERPROFILE\.docker\config.json
# appliquer "credHelpers": {}

# Connecter Docker à ton registre ECR AWS
aws ecr get-login-password --region $REGION --profile $PROFILE | docker login --username AWS --password-stdin "$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com"

# 4. Builder l'image (cela va utiliser ton Dockerfile)
docker build -t $REPO_NAME .

# 5. Tagger l'image pour AWS
docker tag $REPO_NAME:latest "$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/ray-training:latest"

# 6. Pousser l'image sur AWS
docker push "$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$REPO_NAME:latest"

```
### 7.1. Installer Kuberay
```PowerShell
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update
helm install kuberay-operator kuberay/kuberay-operator --version 1.1.0

#vérif
kubectl get crd | Select-String ray
```
<p align="right"><a href="#sommaire">▲ Retour au sommaire</a></p>
### 7.2. Manifest RAY `ray-cluster.yaml`
```yaml
apiVersion: ray.io/v1
kind: RayCluster
metadata:
  name: raycluster-kuberay
  namespace: default
spec:
  rayVersion: '2.9.0'
  headGroupSpec:
    rayStartParams:
      dashboard-host: '0.0.0.0'
    template:
      spec:
        serviceAccountName: mlflow-s3-access
        containers:
          - name: ray-head
            image: <ACCOUNT_ID>.dkr.ecr.eu-west-1.amazonaws.com/ray-training:latest
            imagePullPolicy: Always
            ports:
              - containerPort: 6379
                name: gcs
              - containerPort: 8265
                name: dashboard
              - containerPort: 10001
                name: client
            # resources:
            #   limits:
            #     cpu: "0.5"      # <-- 
            #     memory: "2Gi"   # <-- 
            #   requests:
            #     cpu: "0.5"      # <-- 
            #     memory: "2Gi"   # <-- 
            resources:
              limits:
                cpu: "1"      # <-- changement d'instance
                memory: "4Gi"   # <-- 
              requests:
                cpu: "1"      # <-- 
                memory: "4Gi"   # <-- 
  workerGroupSpecs:
    # ------------------------------------------------------------
    # GROUPE 1 : WORKERS CPU (ML classique - Allégé pour t3.medium)
    # ------------------------------------------------------------
    - replicas: 1
      groupName: small-group
      rayStartParams: {}
      template:
        spec:
          serviceAccountName: mlflow-s3-access
          containers:
            - name: ray-worker-cpu
              image: 963508377363.dkr.ecr.eu-west-1.amazonaws.com/ray-training:latest
            #   resources:
            #     limits:
            #       cpu: "0.5"    # <-- 
            #       memory: "1Gi" # <-- 
            #     requests:
            #       cpu: "0.5"    # <-- 
            #       memory: "1Gi" # <-- 
              resources:
                limits:
                  cpu: "1"    # <-- Modifié (Économique)
                  memory: "2Gi" # <-- Modifié (Économique)
                requests:
                  cpu: "1"    # <-- Modifié (Économique)
                  memory: "2Gi" # <-- Modifié (Économique)
          nodeSelector:
            eks.amazonaws.com/nodegroup: standard-nodes

    # ------------------------------------------------------------
    # GROUPE 2 : WORKERS GPU (Deep Learning - Inchangé à 0 réplica)
    # ------------------------------------------------------------
    - replicas: 0 
      groupName: gpu-group
      rayStartParams: {}
      template:
        spec:
          serviceAccountName: mlflow-s3-access
          containers:
            - name: ray-worker-gpu
              image: 963508377363.dkr.ecr.eu-west-1.amazonaws.com/ray-training:latest
              resources:
                limits:
                  cpu: "3"      
                  memory: "12Gi" 
                  nvidia.com/gpu: "1" 
                requests:
                  cpu: "3"
                  memory: "12Gi"
                  nvidia.com/gpu: "1"
          nodeSelector:
            eks.amazonaws.com/nodegroup: gpu-nodes
            k8s.amazonaws.com/accelerator: nvidia-t4
          tolerations:
          - key: "nvidia.com/gpu"
            operator: "Exists"
            effect: "NoSchedule"
```

```powershell
kubectl apply -f ray-cluster.yaml
kubectl get pods -w


# accéder au dashboard ray
kubectl port-forward svc/raycluster-kuberay-head-svc 8265:8265
```
<p align="right"><a href="#sommaire">▲ Retour au sommaire</a></p>
---

## 8. scaling

### 8.1. Avec un rolling update suite à une modif de machine
```powershell
eksctl upgrade nodegroup 
--config-file=ray-ml-cluster.yaml 
--name=standard-nodes 
--approve 
--profile $PROFILE


# Scaler avec 1 machine de plus
eksctl scale nodegroup `
  --cluster ray-ml-cluster `
  --name standard-nodes `
  --nodes 3 `
  --region $REGION `
  --profile $PROFILE

```
### 8.2. Détruire et recréer le groupe
```powershell
# 1. Supprimer l'ancien pool de petites machines (ça prend 2 min)
eksctl delete nodegroup `
  --cluster=ray-ml-cluster `
  --name=standard-nodes `
  --disable-eviction `
  --region=eu-west-1 `
  --profile $PROFILE

# 2. Créer le nouveau pool à partir de ton fichier modifié (ça prend 3 min)
#changer le nom de la machine dans ray-ml-cluster.yaml par exemple standard-nodes-v2
# et dans ray-cluster.yaml nodeSelector:
# eks.amazonaws.com/nodegroup: standard-nodes-v2
# tuer le noeud problématique
kubectl delete pod raycluster-kuberay-worker-small-group-xxx
eksctl create nodegroup --config-file=ray-ml-cluster.yaml  --profile $PROFILE

# 3. ajouter des gpus
kubectl scale --replicas=1 raycluster/raycluster-kuberay --component=gpu-group
kubectl get pods -w

# Eteindre le GPU
kubectl scale --replicas=0 raycluster/raycluster-kuberay --component=gpu-group

```
<p align="right"><a href="#sommaire">▲ Retour au sommaire</a></p>

## 9. Entraîner un modèle
**tunnel vers le Dashboard Ray**
kubectl port-forward svc/raycluster-kuberay-head-svc 8265:8265
http://localhost:8265

**tunnel vers MLflow**
kubectl port-forward svc/mlflow-service 5000:5000

**Conteneur Head**
```powershell
$HEAD_POD = kubectl get pods -l ray.io/node-type=head -o jsonpath='{.items[0].metadata.name}'
kubectl exec -it $HEAD_POD  -- bash
python /home/ray/train.py

# avec un nouveau script qui n'est pas dans le Dockerfile
kubectl cp .\train_v2.py "${HEAD_POD}:/home/ray/train_v2.py"

# Lancer l'entraînement
python /home/ray/train.py
```
<p align="right"><a href="#sommaire">▲ Retour au sommaire</a></p>
## 10. Suppression des ressources

```powershell
#Supprime le cluster Ray au complet
kubectl delete -f ray-cluster.yaml

# Remets le groupe de nœuds standard au strict minimum
eksctl scale nodegroup `
  --cluster=ray-ml-cluster `
  --name=standard-nodes `
  --nodes=1 `
  --nodes-min=1 `
  --region=eu-west-1 `
  --profile $PROFILE
# supprime le cluster EKS et toutes ses machines
  eksctl delete cluster --name=ray-ml-cluster --region=eu-west-1 --profile $PROFILE
```
Les petits détails à vérifier manuellement sur la console AWS
Une fois le cluster supprimé, l'outil eksctl nettoie 95% des ressources. Par sécurité, va faire un tour sur ta console AWS sur navigateur pour vérifier ces 3 points qui peuvent parfois rester et grignoter quelques centimes :

**Amazon RDS (Base de données)** : dans le service RDS et supprime l'instance (pense à cocher "No" pour le snapshot final si tu n'as plus besoin des données, pour éviter de payer le stockage du snapshot).

**Amazon S3** : AWS ne supprime jamais un bucket S3 qui contient des fichiers (tes artefacts MLflow ou tes checkpoints Ray). Les buckets vides ne coûtent rien, mais si tu as des gigaoctets de modèles stockés, va dans le service S3 pour vider et supprimer ton bucket si tu n'en as plus besoin.

**Amazon ECR (Images Docker)** : ray-training est stockée sur ECR. Le stockage ECR est très peu cher (quelques centimes par Go par mois), mais si tu veux un nettoyage à 100%, va dans ECR et supprime le dépôt ray-training.
<p align="right"><a href="#sommaire">▲ Retour au sommaire</a></p>


