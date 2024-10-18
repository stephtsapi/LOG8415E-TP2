import boto3
import os
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from dotenv import load_dotenv

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

# Récupérer les credentials depuis le fichier .env
ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
SESSION_TOKEN = os.getenv('AWS_SESSION_TOKEN')  
try:
    # Créer une session AWS avec les credentials
    session = boto3.Session(
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        aws_session_token=SESSION_TOKEN  # Utilisé uniquement si défini
    )

    # Connexion au service S3 pour tester la connexion
    s3 = session.resource('s3')

    print("Liste des buckets S3 disponibles :")
    for bucket in s3.buckets.all():
        print(f"- {bucket.name}")

except NoCredentialsError:
    print("Erreur : Les identifiants AWS sont manquants ou incorrects.")
except PartialCredentialsError:
    print("Erreur : Certaines informations d'identifiants AWS sont manquantes.")
except Exception as e:
    print(f"Erreur : {str(e)}")

    ec2 = session.resource('ec2')

def create_ec2_instance(user_id):
    """
    Crée une instance EC2 t2.large avec une paire de clés SSH.

    Args:
        user_id (str): Identifiant de l'utilisateur pour tagger l'instance.
    
    Returns:
        dict: ID et IP publique de l'instance.
    """
    try:
        # Générer une paire de clés SSH
        key_name = f"my-key-pair-{user_id}"
        key_pair = ec2.create_key_pair(KeyName=key_name)

        # Sauvegarder la clé privée au format .pem
        pem_file = f"{key_name}.pem"
        with open(pem_file, "w") as file:
            file.write(key_pair.key_material)
        print(f"Clé privée SSH enregistrée sous : {pem_file}")

        # Modifier les permissions de la clé privée
        os.chmod(pem_file, 0o400)

        # Créer une instance EC2 avec la paire de clés générée
        instances = ec2.create_instances(
            ImageId='ami-0c55b159cbfafe1f0',  # AMI Amazon Linux 2 (USA East)
            MinCount=1,
            MaxCount=1,
            InstanceType='t2.large',
            KeyName=key_name,
            TagSpecifications=[{
                'ResourceType': 'instance',
                'Tags': [
                    {'Key': 'Name', 'Value': f'MyLinuxInstance-{user_id}'}
                ]
            }]
        )

        # Attendre que l'instance soit en cours d'exécution
        instance = instances[0]
        print("Création de l'instance en cours...")
        instance.wait_until_running()

        # Actualiser les informations de l'instance
        instance.load()
        print(f"Instance créée avec succès ! ID : {instance.id}")
        print(f"Adresse IP publique : {instance.public_ip_address}")

        return {
            "instance_id": instance.id,
            "public_ip": instance.public_ip_address
        }

    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"Erreur de connexion AWS : {str(e)}")
    except Exception as e:
        print(f"Une erreur s'est produite : {str(e)}")

# Exemple d'utilisation de la fonction
if __name__ == "__main__":
    # Remplacez "user123" par l'identifiant que vous voulez utiliser
    result = create_ec2_instance("user123")
    if result:
        print(f"Instance ID: {result['instance_id']}")
        print(f"Adresse IP Publique: {result['public_ip']}")