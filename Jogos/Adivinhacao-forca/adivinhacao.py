def jogar():
    import random
    print("********************************")
    print("Bem vindo ao Jogo de Adivinhação")
    print("********************************")

    while(True):

        print("\nEm qual nível de dificuldade deseja jogar?\n(1) Fácil\n(2) Médio\n(3) Difícil")
        nivel = int(input("\nDigite o nível: "))

        if(nivel == 1):
            numero_secreto = random.randrange(1,11) #Define um número aleatório entre 0  e 10
            total_de_tentativas = 5
            print("\nTente adivinhar o número secreto, o número se encontra entre 0 e 10", end=".")
            break
        elif(nivel == 2):
            numero_secreto = random.randrange(1,51) #Define um número aleatório entre 0  e 50
            total_de_tentativas = 6
            print("\nTente adivinhar o número secreto, o número se encontra entre 0 e 50", end=".")
            break
        elif(nivel == 3):
            numero_secreto = random.randrange(1,101) #Define um número aleatório entre 0  e 100
            total_de_tentativas = 6
            print("\nTente adivinhar o número secreto, o número se encontra entre 0 e 100", end=".")
            break
        else:
            print("\nOpção digitada inválida, Escolha novamente")



    for rodada in range(1,total_de_tentativas+1):

        print("Tentativa {} de {}\n".format(rodada,total_de_tentativas))
        chute = input("Digite seu numero: ")
        chute = int(chute)

        acertou = chute == numero_secreto
        maior = chute > numero_secreto
        menor = chute < numero_secreto

        if(acertou):
            print("\n***Você acertou***\n")
            break
        else:
            if(maior):
                print("O número digitado é maior que o secreto\n")
            elif(menor):
                print("O número digitado é menor que o secreto\n")
            else:
                print("O valor digitado é incorreto\n")


    print("Fim de jogo, o número secreto é {}" .format(numero_secreto))

if(__name__ == "__main__"):
    jogar()
