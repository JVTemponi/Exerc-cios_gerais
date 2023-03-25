package CalculadoraLambda;
import java.util.Scanner;

public class AppCalculadora {

    static Scanner entrada = new Scanner(System.in);
    
    public static void main(String[] args) {
        
        double resultado;
        int operacao = menuOpcoes();
        double[] numeros = escolherNumeros();
        resultado = operacionar(operacao, numeros[0], numeros[1]);
        
        System.out.println("\n----------------------------------------");
        System.out.println("O resultado da operação é: " + resultado);
        System.out.println("----------------------------------------");
    }


    public static double operacionar(int opcao, double primeiroNumero, double segundoNumero){

        ICalcular soma, sub, mul, div;
        soma = (x,y) -> x+y;
        sub = (x,y) -> x-y;
        mul = (x,y) -> x*y;
        div = (x,y) -> x/y;

        switch (opcao) {
            case 1:
                return soma.calcular(primeiroNumero, segundoNumero);
            case 2:
                return sub.calcular(primeiroNumero, segundoNumero);
            case 3:
                return mul.calcular(primeiroNumero, segundoNumero);
            case 4:
                return div.calcular(primeiroNumero, segundoNumero);   
            default:
                return 0;
        }
    }

    public static int menuOpcoes(){
        
        int opcao = 0;

        while(true){

            System.out.println("\nSelecione o número da operação desejada");
            System.out.println("1 -> Soma entre dois números");
            System.out.println("2 -> Subtração entre dois números");
            System.out.println("3 -> Multiplicação entre dois números");
            System.out.println("4 -> Divisão entre dois números");
            
            try {
                opcao = Integer.parseInt(entrada.nextLine());
                break;
            } catch (NumberFormatException e) {
                System.out.println("Selecione uma opção válida!");
            }
        }
        return opcao;
    }

    public static double[] escolherNumeros(){

        double[] numeros = new double[2];

        while(true){

            System.out.println("\nDigite os valores que deseja calcular de acordo com os exemplos abaixo");
            System.out.println("Exemplos: (x + y)   |   (x-y)  |  (x*y)  |  (x/y)\n");

            try {
                System.out.println("Valor para x: ");
                numeros[0] = Double.parseDouble(entrada.nextLine().replace(",", "."));
                System.out.println("Valor para y: ");
                numeros[1] = Double.parseDouble(entrada.nextLine().replace(",", "."));
                break;
            } catch (NumberFormatException e) {
                System.out.println("Digite um número válido!");
            }
        }
        return numeros;
    }

    
}

