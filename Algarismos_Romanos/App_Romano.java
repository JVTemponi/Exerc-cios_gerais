import java.util.ArrayList;
import java.util.Scanner;

public class App_Romano{
    public static void main(String[] args) {

        String valorAserTransformado;
        Algarismos_Romanos numeroRomano;
        Scanner valorDigitado = new Scanner(System.in);
        int valor = 0;

        System.out.printf("\n\nDigite o Número Romano desejado: ");
        valorAserTransformado = valorDigitado.nextLine();

        numeroRomano = new Algarismos_Romanos(valorAserTransformado);
        valor = numeroRomano.getValorEmNumerais();

        System.out.println("O valor digitado é: " + valor);

    }
}