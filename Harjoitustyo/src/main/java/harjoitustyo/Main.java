
package harjoitustyo;

import java.sql.*;
import java.util.Scanner;

 //@author hiira
 
public class Main {
    private static Scanner lukija = new Scanner(System.in);
    private static DAO kanta = new DAO("htkanta.db");
    
    private static void lisaaPaikka() {
        System.out.print("Anna paikan nimi: ");
        String paikanNimi = lukija.nextLine();
        kanta.lisaaPaikka(paikanNimi);
    }
    
    private static void lisaaAsiakas() {
        System.out.print("Anna asiakkaan nimi: ");
        String asiakkaanNimi = lukija.nextLine();
        kanta.lisaaAsiakas(asiakkaanNimi);
    }
    
    private static void lisaaPaketti() {
        System.out.print("Anna lisättävän paketin seurantakoodi: ");
        String seurantakoodi = lukija.nextLine();
        System.out.print("Anna asiakkaan nimi: ");
        String nimi = lukija.nextLine();
        kanta.lisaaPaketti(seurantakoodi,nimi);
    }
    
    private static void lisaaTapahtuma() {
        System.out.print("Anna paketin seurantakoodi: ");
        String koodi = lukija.nextLine();
        System.out.print("Anna tapahtuman paikka: ");
        String paikka = lukija.nextLine();
        System.out.print("Anna tapahtuman kuvaus: ");
        String kuvaus = lukija.nextLine();
        kanta.lisaaTapahtuma(koodi,paikka,kuvaus);
    }
    
    private static void haePaketinTapahtumat() {
        System.out.println("Paketin tapahtumien haku.");
        System.out.print("Anna paketin seurantakoodi: ");
        String koodi = lukija.nextLine();
        kanta.haePaketinTapahtumat(koodi);
    }
    
    private static void haeAsiakkaanPaketit() {
        System.out.println("Asiakkaan pakettien haku.");
        System.out.print("Anna asiakkaan nimi: ");
        String asiakas = lukija.nextLine();
        kanta.haeAsiakkaanPaketit(asiakas);
    }
    
    private static void haePaikanTapahtumat() {
        System.out.println("Paikan tapahtumien haku tiettynä päivänä.");
        System.out.print("Anna paikan nimi: ");
        String paikka = lukija.nextLine();
        System.out.print("Anna päivämäärä (d.m.yyyy): ");
        String paiva = lukija.nextLine();
        kanta.haePaikanTapahtumat(paikka,paiva);
    }
    
    public static void main(String[] args) {
            //valintavalikko
            System.out.println("PAKETTISEURANTA");
            System.out.println("");
            System.out.println("1 - luo taulut tietokantaan");
            System.out.println("2 - lisää uusi paikka tietokantaan");
            System.out.println("3 - lisää uusi asiakas tietokantaan");
            System.out.println("4 - lisää uusi paketti tietokantaan");
            System.out.println("5 - lisää uusi tapahtuma tietokantaan");
            System.out.println("6 - hae kaikki paketin tapahtumat "
                    + "seurantakoodin perusteella");
            System.out.println("7 - hae kaikki asiakkaan paketit ja niihin "
                    + "liittyvien tapahtumien määrä");
            System.out.println("8 - hae annetusta paikasta tapahtumien "
                    + "määrä tiettynä päivänä");
            System.out.println("9 - suorita tietokannan tehokkuustesti");
            System.out.println("0 - lopeta ohjelman suoritus");
           
        while (true) {
  
            System.out.println("");
            System.out.print("Valitse toiminto: ");
            String komento = lukija.nextLine();
            //int komento = Integer.valueOf(syote);
            
            System.out.println("");
            
            switch (komento) {
                case "1":
                    kanta.luoTaulut();    
                    break;
                case "2":
                    lisaaPaikka();
                    break;
                case "3":
                    lisaaAsiakas();
                    break;
                case "4":
                    lisaaPaketti();
                    break;
                case "5":
                    lisaaTapahtuma();
                    break;
                case "6":
                    haePaketinTapahtumat();
                    break;
                case "7":
                    haeAsiakkaanPaketit();
                    break;
                case "8":
                    haePaikanTapahtumat();
                    break;
                case "9":
                    DAO tehokanta = new DAO("tehokkuustestikanta.db");
                    tehokanta.suoritaTehokkuustesti();
                    break;
                case "0":
                    System.exit(0);
                case "10":
                    kanta.tulostaTietokanta();
                    break;
                case "11":
                    kanta.poistaTaulut();
                    break;
                default:
                    System.out.println("Virheellinen komento");
                    break;
            }
        }
    }
}
