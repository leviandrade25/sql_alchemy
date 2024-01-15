import os

# Definindo variáveis de ambiente
os.environ['MYAPP_USERNAME'] = 'meu_usuario'
os.environ['MYAPP_PASSWORD'] = 'minha_senha'

# Acessando as variáveis de ambiente
username = os.environ.get('MYAPP_USERNAME')
password = os.environ.get('MYAPP_PASSWORD')

# Usando as variáveis no seu código
print(f"Usuário: {username}, Senha: {password}")
