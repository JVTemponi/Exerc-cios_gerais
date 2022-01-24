import forca
import adivinhacao

print("********************************")
print("***     Escolha seu jogo!    ***")
print("********************************")

print("\nSelecione qual jogo deseja jogar:\n(1) Adivinhação\n(2) Forca")
opcao = int(input("\n\nDigite a opção escolhida: "))

if(opcao == 1):
    adivinhacao.jogar()
elif(opcao == 2):
    forca.jogar()

print(opcao)