def jogar():

    print("********************************")
    print("Bem vindo ao jogo da Forca")
    print("********************************")

    palavra_secreta = "python"

    enforcou = False
    acertou = False
    errou = 0

    while(not enforcou and not acertou):

        print("Tente adivinhar a plavra e tire o boneco da forca.")
        letra_escolhida = input("Escolha uma letra: ")

        posicao = 0
        for letra in palavra_secreta:
            if(letra_escolhida == letra):
                print("A letra {} está correta sendo a {} letra da palavra secreta".format(letra_escolhida, posicao +1))
            else:
                errou = errou +1

            posicao = posicao + 1

            if(errou == 6):
                enforcou = True

    print("Fim de jogo")


if(__name__ == "__main__"):
    jogar()