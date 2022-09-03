import java.util.ArrayList;

public class Algarismos_Romanos {
    private String numeroRomano;
    private ArrayList<String> algarismosRomanosSeparados = new ArrayList<String>();
    private int valorEmNumerais;

    Algarismos_Romanos(String numeroRomano){
        
        if (validarNumeroRomano(numeroRomano)) {
            this.numeroRomano = numeroRomano;
            this.valorEmNumerais = 0;
            separaAlgarismos(numeroRomano);
            transformaEmNumeros(algarismosRomanosSeparados);
        }
        else{
            this.numeroRomano = "0";
            this.valorEmNumerais = 0;
            this.algarismosRomanosSeparados = new ArrayList<String>();
        }
    }

    public String getNumeroRomano() {

        return numeroRomano;
    }

    public int getValorEmNumerais() {
        return valorEmNumerais;
    }

    private boolean validarNumeroRomano(String numeroRomano){
        boolean numeroValido = false;

        if (!numeroRomano.contains("I") && !numeroRomano.contains("V") && !numeroRomano.contains("X") && !numeroRomano.contains("M") && !numeroRomano.contains("C") && !numeroRomano.contains("D")){
            numeroValido = false;
        }
        else{
            numeroValido = true;
        }
        return numeroValido;
    }

    private void separaAlgarismos(String numeroRomano){
        ArrayList<String> valoresSeparado = new ArrayList<String>();
            
        for(int i = 0; i < numeroRomano.length(); i++){
            
            char caractere = numeroRomano.charAt(i);
            String caractereTexto = Character.toString(caractere);

            if ((numeroRomano.contains("IV") && i == 0)){
                this.valorEmNumerais = this.valorEmNumerais -2;
                valoresSeparado.add(caractereTexto);
                
            }
            else if ((numeroRomano.contains("IX") && i == 0)){
                this.valorEmNumerais = this.valorEmNumerais -2;
                valoresSeparado.add(caractereTexto);
                
            }
            else if ((numeroRomano.contains("XL") && i == 0)){
                this.valorEmNumerais = this.valorEmNumerais -20;
                valoresSeparado.add(caractereTexto);
                
            }
            else if ((numeroRomano.contains("XC") && i == 0)){
                this.valorEmNumerais = this.valorEmNumerais -20;
                valoresSeparado.add(caractereTexto);
                
            }
            else if ((numeroRomano.contains("CD") && i == 0)){
                this.valorEmNumerais = this.valorEmNumerais -200;
                valoresSeparado.add(caractereTexto);
                
            }
            else if ((numeroRomano.contains("CM") && i == 0)){
                this.valorEmNumerais = this.valorEmNumerais -200;
                valoresSeparado.add(caractereTexto);
                
            }
            else{      
                valoresSeparado.add(caractereTexto);
            }
        }

        this.algarismosRomanosSeparados.addAll(valoresSeparado);

    }

    private void transformaEmNumeros(ArrayList<String> algarismosRomanos){
        for(int i = 0; i < algarismosRomanos.size(); i++) {
            
            String posicao = algarismosRomanos.get(i);
            
            switch (posicao) {
                case "I":
                    this.valorEmNumerais = this.valorEmNumerais + 1;
                    break;

                case "V":
                    this.valorEmNumerais = this.valorEmNumerais + 5;
                    break;

                case "X":
                    this.valorEmNumerais = this.valorEmNumerais + 10;
                    break;

                case "L":
                    this.valorEmNumerais = this.valorEmNumerais + 50;
                    break;

                case "C":
                    this.valorEmNumerais = this.valorEmNumerais + 100;
                    break;

                case "D":
                    this.valorEmNumerais = this.valorEmNumerais + 500;
                    break;

                case "M":
                    this.valorEmNumerais = this.valorEmNumerais + 1000;
                    break;

                default:
                    break;
            }
        }
    }

}