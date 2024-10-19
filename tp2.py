import boto3
import os
import uuid
import paramiko
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from dotenv import load_dotenv

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

# Récupérer les credentials depuis le fichier .env
ACCESS_KEY = os.getenv('aws_access_key_id')
SECRET_KEY = os.getenv('aws_secret_access_key')
SESSION_TOKEN = os.getenv('aws_session_token')  
REGION_NAME = os.getenv('aws_region')
try:
    # Créer une session AWS avec les credentials
    session = boto3.Session(
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        aws_session_token=SESSION_TOKEN, # Utilisé uniquement si défini
        region_name=REGION_NAME  # Spécifiez la région ici
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

def get_default_ami(region_name):
    """
    Récupère un ID d'AMI par défaut pour la région spécifiée.

    Args:
        region_name (str): Nom de la région AWS.

    Returns:
        str: ID de l'AMI.
    """
    ec2_client = boto3.client('ec2', region_name=region_name)
    response = ec2_client.describe_images(
        Owners=['amazon'],
        Filters=[
            {'Name': 'name', 'Values': ['amzn2-ami-hvm-*-x86_64-gp2']},
            {'Name': 'state', 'Values': ['available']}
        ]
    )
    images = sorted(response['Images'], key=lambda x: x['CreationDate'], reverse=True)
    return images[0]['ImageId'] if images else None

def create_ec2_instance():
    """
    Crée une instance EC2 avec un nom et un ID aléatoires, et génère une paire de clés SSH unique.

    Returns:
        dict: ID et IP publique de l'instance.
    """
    try:
        # Initialiser la ressource EC2
        ec2 = boto3.resource('ec2')
        region_name = boto3.Session().region_name

        # Obtenir l'AMI par défaut pour la région actuelle
        ami_id = get_default_ami(region_name)
        if not ami_id:
            raise Exception("Aucune AMI valide trouvée pour la région actuelle.")

        # Générer un nom et un ID aléatoires pour l'instance
        instance_id = str(uuid.uuid4())
        instance_name = f"instance-{instance_id}"

        # Générer une paire de clés SSH unique
        key_name = f"my-key-pair-{instance_id}"
        ec2_client = boto3.client('ec2', region_name=region_name)

        # Vérifier si la paire de clés existe déjà
        try:
            ec2_client.describe_key_pairs(KeyNames=[key_name])
            print(f"La paire de clés {key_name} existe déjà.")
        except ec2_client.exceptions.ClientError as e:
            if 'InvalidKeyPair.NotFound' in str(e):
                key_pair = ec2.create_key_pair(KeyName=key_name)
                # Sauvegarder la clé privée au format .pem
                pem_file = f"{key_name}.pem"
                with open(pem_file, "w") as file:
                    file.write(key_pair.key_material)
                print(f"Clé privée SSH enregistrée sous : {pem_file}")
                # Modifier les permissions de la clé privée
                os.chmod(pem_file, 0o400)
            else:
                raise

        # Créer une instance EC2 avec la paire de clés générée
        instances = ec2.create_instances(
            ImageId=ami_id,  # Utiliser l'AMI par défaut pour la région actuelle
            MinCount=1,
            MaxCount=1,
            InstanceType='t2.large',
            KeyName=key_name,
            TagSpecifications=[{
                'ResourceType': 'instance',
                'Tags': [
                    {'Key': 'Name', 'Value': instance_name}
                ]
            }]
        )

        # Attendre que l'instance soit en cours d'exécution
        instance = instances[0]
        print("Création de l'instance en cours...")
        instance.wait_until_running()
        instance.load()
        return {
            'InstanceId': instance.id,
            'PublicIpAddress': instance.public_ip_address,
            'key_name': key_name
        }

    except Exception as e:
        print(f"Une erreur s'est produite : {e}")
        return None

def install_hadoop(instance_ip, key_file): # Fonction pour installer Hadoop sur l'instance EC2
    """
    Installe Hadoop sur une instance EC2 distante.

    Args:
        instance_ip (str): Adresse IP publique de l'instance EC2.
        key_file (str): Chemin vers le fichier de clé privée SSH.

    Returns:
        bool: True si l'installation a réussi, False sinon.
    """
    try:
        # Connexion SSH à l'instance EC2 à partir de la clé privée .pem
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname=instance_ip, username='ec2-user', key_filename=key_file)

        print("Connexion SSH établie avec succès.")

        # Commandes d'installation de Hadoop
        commands = [
            "sudo apt update",
            "sudo apt install default-jdk",
            "java -version",
            "wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz",
            "wget https://downloads.apache.org/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz.sha512",
            "shasum -a 512 hadoop-3.3.1.tar.gz",
            "cat hadoop-3.3.1.tar.gz.sha512",
            "tar -xzvf hadoop-3.3.1.tar.gz",
            "sudo mv hadoop-3.3.1 /usr/local/hadoop",
            "echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/' >> ~/usr/local/hadoop/etc/hadoop/hadoop-env.sh",
            "echo 'export PATH=$PATH:$HADOOP_HOME/bin' >> ~/usr/local/hadoop/etc/hadoop/hadoop-env.sh",
            "/usr/local/hadoop/bin/hadoop",
            "mkdir ~/input",
            "cp /usr/local/hadoop/etc/hadoop/*.xml ~/input",
            "/usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.1.jar grep ~/input ~/grep_example 'allowed[.]*'",
            "cat ~/grep_example/*",
        ]

        # Exécuter les commandes sur l'instance distante
        for command in commands:
            stdin, stdout, stderr = ssh_client.exec_command(command)
            output = stdout.read().decode('utf-8')
            error = stderr.read().decode('utf-8')
            if error:
                print(f"Erreur lors de l'exécution de la commande : {error}")
                return False
            print(output)

        print("Installation de Hadoop terminée avec succès.")
        return True

    except Exception as e:
        print(f"Une erreur s'est produite : {e}")
        return False

def install_spark(instance_ip, key_file): # Fonction pour installer Spark sur l'instance EC2   



    """
    Installe Spark sur une instance EC2 distante.

    Args:
        instance_ip (str): Adresse IP publique de l'instance EC2.
        key_file (str): Chemin vers le fichier de clé privée SSH.

    Returns:
        bool: True si l'installation a réussi, False sinon.
    """
    try:
        # Connexion SSH à l'instance EC2 que je viens de créer
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname=instance_ip, username='ec2-user', key_filename=key_file)

        print("Connexion SSH établie avec succès.")

        # Commandes d'installation de Spark
        commands = [
            "wget https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz",
            "tar -xvzf spark-3.5.1-bin-hadoop3.tgz",
            "sudo mv spark-3.5.1-bin-hadoop3 /opt/spark",
            "echo 'export SPARK_HOME=/opt/spark/spark-3.5.1-bin-hadoop3' >> ~/.bashrc",
            "echo 'export PATH=$PATH:$SPARK_HOME/bin' >> ~/.bashrc",
            "source ~/.bashrc",
            "spark-shell --version",
        ]

        # Exécuter les commandes sur l'instance distante
        for command in commands:
            stdin, stdout, stderr = ssh_client.exec_command(command)
            output = stdout.read().decode('utf-8')
            error = stderr.read().decode('utf-8')
            if error:
                print(f"Erreur lors de l'exécution de la commande : {error}")
                return False
            print(output)

        print("Installation de Spark terminée avec succès.")
        return True

    except Exception as e:
        print(f"Une erreur s'est produite : {e}")
        return False
    
def tp2():
    """
    Fonction principale pour le TP2.
    """
    # Créer une instance EC2
    result = create_ec2_instance()
    if result:
        print(f"Instance créée avec succès : {result}")

    # Installer Hadoop sur l'instance EC2
    # hadoop=install_hadoop(result['PublicIpAddress'], f"{result['key_name']}.pem")
    # if hadoop:
     #   print("Hadoop a été installé avec succès.")
    #else:
     #   print("Erreur lors de l'installation de Hadoop.")
    # Installer Spark sur l'instance EC2
    #spark=install_spark(result['PublicIpAddress'], f"{result['key_name']}.pem")
    #if spark:
    #    print("Spark a été installé avec succès.")
    #else:
    #    print("Erreur lors de l'installation de Spark.")

# Exécution la fonction principale
tp2()


    

