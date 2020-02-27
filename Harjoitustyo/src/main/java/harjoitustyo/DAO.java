/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package harjoitustyo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;
import java.util.Random;

/**
 *
 * @author hiira
 */
public class DAO {
    final String nimi;
    
    //konstruktori
    public DAO(String tietokannanNimi) {
        this.nimi=tietokannanNimi;
    }
    
    //metodi, joka luo taulut tietokantaan
    public void luoTaulut() {
        try (Connection db = DriverManager.getConnection("jdbc:sqlite:"+this.nimi)) {           
            Statement s = db.createStatement();
            s.execute("CREATE TABLE IF NOT EXISTS Asiakkaat "
                    + "(id INTEGER PRIMARY KEY, nimi TEXT UNIQUE)");
            s.execute("CREATE TABLE IF NOT EXISTS Paikat "
                    + "(id INTEGER PRIMARY KEY, nimi TEXT UNIQUE)");
            s.execute("CREATE TABLE IF NOT EXISTS Paketit "
                    + "(id INTEGER PRIMARY KEY, seurantakoodi TEXT UNIQUE, "
                    + "asiakas_id INTEGER)");
            s.execute("CREATE TABLE IF NOT EXISTS Tapahtumat "
                    + "(id INTEGER PRIMARY KEY, paketti_id INTEGER, "
                    + "paikka_id INTEGER, kuvaus TEXT, aika TEXT)");
        
        } catch (SQLException e) {
            System.err.println("Tietokantataulujen luonnissa tapahtui virhe.");
            getErrorMessages(e);
        }  
    }
    
    
    
    public void poistaTaulut() {
        try (Connection db = DriverManager.getConnection("jdbc:sqlite:"+this.nimi)) {
            Statement s = db.createStatement();
            s.execute("DROP TABLE IF EXISTS Asiakkaat");
            s.execute("DROP TABLE IF EXISTS Paikat");
            s.execute("DROP TABLE IF EXISTS Paketit");
            s.execute("DROP TABLE IF EXISTS Tapahtumat");
        } catch (SQLException e) {
            System.out.println("Taulujen poistossa tapahtui virhe.");
            getErrorMessages(e);
        }
    }
    
    public void lisaaPaikka(String paikka) {
        try (Connection db = DriverManager.getConnection("jdbc:sqlite:"+this.nimi)) {
             //aloitetaan transaktio, jotta vältytään samanaikaisuusogelmalta 
             // tarkistuksen ja lisäämisen välissä
            Statement st = db.createStatement();
            st.execute("BEGIN TRANSACTION");
            
            //varmitetaan ensin, että paikkaa ei ole jo olemassa
            PreparedStatement s = db.prepareStatement("SELECT * "
                    + "FROM Paikat WHERE nimi=?");
            s.setString(1, paikka);
            ResultSet resultset = s.executeQuery();
            
            if(resultset.next()) {
                System.out.println("Paikka on olemassa jo.");
                st.execute("COMMIT");
                return;
            }
            
            //lisätään uusi paikka tauluun Paikat
            s = db.prepareStatement("INSERT INTO Paikat (nimi) VALUES (?)");
            s.setString(1, paikka);
            s.executeUpdate();
            
            //lopetetaan transaktio
            st.execute("COMMIT");
            
            System.out.println("Paikka " + paikka + " lisätty.");
            
        } catch (SQLException e) {
            System.err.println("Paikan lisääminen ei onnistu.");
            getErrorMessages(e);
        }
    }
    
    public void lisaaAsiakas(String nimi) {
        try (Connection db = DriverManager.getConnection("jdbc:sqlite:"+this.nimi)) {
            //aloitetaan transaktio, jotta tarkastuksessa vältytään 
            //samanaikaisuusongelmalta
            Statement st = db.createStatement();
            st.execute("BEGIN TRANSACTION");
            
            //varmistetaan ensin, ettei kyseisen nimistä asiakasta ole jo olemassa
            PreparedStatement pre = db.prepareStatement("SELECT * "
                    + "FROM Asiakkaat WHERE nimi=?");
            pre.setString(1, nimi);
            ResultSet rs = pre.executeQuery();
            
            if (rs.next()) {
                System.out.println("VIRHE: Asiakas on jo olemassa.");
                st.execute("COMMIT");
                return;
            }
            
            //lisätään uusi asiakas tauluun Asiakaat
            pre=db.prepareStatement("INSERT INTO Asiakkaat (nimi) VALUES (?)");
            pre.setString(1, nimi);
            pre.executeUpdate();
            
            //lopetetaan transaktio
            st.execute("COMMIT");
            
            System.out.println("Asiakas " + nimi + " lisätty.");
            
        } catch (SQLException e) {
            System.out.println("Asiakasta ei saatu lisättyä.");
            getErrorMessages(e);
        }
    }
    
    public void lisaaPaketti(String koodi, String nimi) {
        try (Connection db = DriverManager.getConnection("jdbc:sqlite:"+this.nimi)) {
            //aloitetaan transaktio
            Statement st = db.createStatement();
            st.execute("BEGIN TRANSACTION");
            
            //Tarkastetaan, että seurantakoodia ei ole jo olemassa
            PreparedStatement s = db.prepareStatement("SELECT * FROM Paketit "
                    + "WHERE seurantakoodi=?");
            s.setString(1,koodi);
            
            ResultSet rs = s.executeQuery();
            if (rs.next()) {
                System.out.println("VIRHE: annetulla seurantakoodilla on jo "
                        + "paketti järjestelmässä.");
                st.execute("COMMIT");
                return;
            }
            //Tarkastetaan, että asiakas on jo tietokannassa
            if(haeAsiakkaanId(nimi)==-1) {
                System.out.println("VIRHE: Asiakasta ei löydy tietokannasta.");
                st.execute("COMMIT");
                return;
            }
            
            //lisätään uusi paketti tietokantaan
            s=db.prepareStatement("INSERT INTO Paketit "
                    + "(seurantakoodi,asiakas_id) VALUES (?,?)");
            s.setString(1,koodi);
            s.setInt(2,haeAsiakkaanId(nimi));
            s.executeUpdate();
            
            //lopetetaan transaktio
            st.execute("COMMIT");
            
            System.out.println("Paketti lisättiin seurantakoodilla "+koodi+".");
            
        } catch (SQLException e) {
            System.out.println("Pakettia ei saada lisättyä.");
            getErrorMessages(e);
        }
    }
    
    public void lisaaTapahtuma(String koodi, String paikka, String kuvaus) {
        try (Connection db = DriverManager.getConnection("jdbc:sqlite:"+this.nimi)) {
            
            //tarkastetaan, että paketti löytyy tietokannasta
            if(haePaketinId(koodi)==-1) {
                System.out.println("VIRHE: Pakettia ei löydy tietokannasta.");
                return;
            }
            
            //tarkastetaan, että paikka löytyy tietokannasta
            if(haePaikanId(paikka)==-1) {
                System.out.println("VIRHE: Paikkaa ei löydy tietokannasta.");
                return;
            }
            
            //lisätään tapahtuma tietokantaan
            PreparedStatement s=db.prepareStatement("INSERT INTO Tapahtumat "
                    + "(paketti_id,paikka_id,kuvaus,aika) VALUES (?,?,?,?)");
            s.setInt(1, this.haePaketinId(koodi));
            s.setInt(2, this.haePaikanId(paikka));
            s.setString(3,kuvaus);
            s.setString(4,this.haeAika());
            
            s.executeUpdate();
            
            System.out.println("Uusi tapahtuma lisättiin.");
            
        } catch (SQLException e) {
            System.out.println("Tapahtuman lisääminen ei onnistunut.");
            getErrorMessages(e);
        }
    }
    
    public void haePaketinTapahtumat(String koodi) {
        try (Connection db = DriverManager.getConnection("jdbc:sqlite:"+this.nimi)) {
            //tarkastetaan, että annettua koodia vastaa jokin paketti tietokannassa
            PreparedStatement s = db.prepareStatement("SELECT * FROM Paketit "
                    + "WHERE seurantakoodi=?");
            s.setString(1,koodi);
            ResultSet rs = s.executeQuery();
            if(!rs.next()) {
                System.out.println("Annettua seurantakoodia vastaavaa "
                        + "pakettia ei löydy tietokannasta.");
                return;
            }
            
            //haetaan paketin tapahtumat
            s=db.prepareStatement("SELECT T.aika, PA.nimi, T.kuvaus "
                    + "FROM Tapahtumat T "
                    + "LEFT JOIN Paketit P ON T.paketti_id=P.id "
                    + "LEFT JOIN Paikat PA ON T.paikka_id=PA.id "
                    + "WHERE P.seurantakoodi=?");
            s.setString(1, koodi);
            rs=s.executeQuery();
            while (rs.next()) {
                System.out.println(rs.getString("aika")+", "+rs.getString("nimi")+", "+rs.getString("kuvaus"));
            }
            
        } catch (SQLException e) {
            System.out.println("Paketin tapahtumien haussa tapahtui virhe.");
            getErrorMessages(e);
        }
    }
    
    public void haeAsiakkaanPaketit(String asiakas) {
        try (Connection db = DriverManager.getConnection("jdbc:sqlite:"+this.nimi)) {
           
            //Tarkastetaan, että annettu asiakas on olemassa
            if (haeAsiakkaanId(asiakas)==-1) {
                System.out.println("VIRHE: asiakasta ei löydy tietokannasta.");
                return;
            }
            //haetaan Asiakkaan paketit ja tapahtumien määrä
            PreparedStatement s = db.prepareStatement("SELECT P.seurantakoodi sk, COUNT(T.id) maara "
                    + "FROM Paketit P"
                    + " LEFT JOIN Tapahtumat T ON T.paketti_id=P.id "
                    + "WHERE P.asiakas_id=(SELECT A.id FROM Asiakkaat A WHERE A.nimi=?)"
                    + " GROUP BY P.seurantakoodi");
            s.setString(1,asiakas);
            
            //tulostetaan paketit ja tapahtumien määrä
            ResultSet rs = s.executeQuery();
            while (rs.next()) {
                System.out.println(rs.getString("sk")+", "+rs.getInt("maara")+" tapahtumaa");
            }
            
        } catch (SQLException e) {
            System.out.println("Asiakkaan pakettien haussa tapahtui virhe.");
            getErrorMessages(e);
        }
    }
    
    public void haePaikanTapahtumat(String paikka, String paiva) {
        try (Connection db = DriverManager.getConnection("jdbc:sqlite:"+this.nimi)) {
            //tarkastetaan onko paikka olemassa
            if (haePaikanId(paikka)==-1) {
                System.out.println("VIRHE: annettua paikkaa ei löydy tietokannasta.");
                return;
            }
            
            //haetaan Paikan tapahtumat annettuna päivänä
            PreparedStatement s = db.prepareStatement("SELECT COUNT(T.kuvaus) m FROM Tapahtumat T "
                    + "WHERE T.paikka_id=(SELECT id FROM Paikat WHERE nimi=?) "
                    + "AND DATE(T.aika)=DATE(?)");
            s.setString(1,paikka);
            s.setString(2,formatoiPaivamaara(paiva));
            
            //tulostetaan
            ResultSet rs = s.executeQuery();
            while (rs.next()) {
                System.out.println("Paikan "+paikka+" tapahtumien määrä annettuna päivämääränä: " + rs.getInt("m"));
            }
            
        } catch (SQLException e) {
            System.out.println("Virhe Paikan tapahtumien haussa tiettynä päivänä.");
            getErrorMessages(e);
        }
        
    }
    
    public void suoritaTehokkuustesti() {
        System.out.println("TEHOKKUUSTESTI");
        System.out.println("");
        System.out.println("Ilman indeksejä:");
        tehokkuustesti(false);
        System.out.println("");
        System.out.println("Indeksien kanssa:");
        tehokkuustesti(true);
    }
    
    
    public void tehokkuustesti(boolean onkoIndeksointi) {
        
        try (Connection db = DriverManager.getConnection("jdbc:sqlite:tehokkuustestikanta.db")) {

            //luodaan taulut tietokantaan, poistetaan jos jo olemassa
            poistaTaulut();
            luoTaulut();
      
            //indekointi
            if (onkoIndeksointi) {
                indeksoi();
            }
            
            
            //aloitetaan transaktio ja alkioiden lisääminen
            Statement s = db.createStatement();
            s.execute("BEGIN TRANSACTION");
            
            
            //Paikat
            long alku = System.nanoTime();
            PreparedStatement p = db.prepareStatement("INSERT INTO Paikat (nimi) VALUES (?)");
            for (int i=1; i<1000 ; i++) {
                p.setString(1,"P"+i);
                p.executeUpdate();
            }
            long loppu = System.nanoTime();
            
            System.out.println("Paikkojen (1000 kpl) lisäämiseen kului: " + ((loppu-alku)/1e9) +" s");
            
            
            //Asiakkaat
            alku=System.nanoTime();
            p=db.prepareStatement("INSERT INTO Asiakkaat (nimi) VALUES (?)");
            for (int i=1 ; i<=1000 ; i++) {
                p.setString(1,"A"+i);
                p.executeUpdate();
            }
            loppu=System.nanoTime();
            
            System.out.println("Asiakkaiden (1000 kpl) lisäämiseen kului: "+((loppu-alku)/1e9)+" s");
            
            
            //Paketit
            alku=System.nanoTime();
            p=db.prepareStatement("INSERT INTO Paketit (seurantakoodi,asiakas_id) VALUES (?,(SELECT id FROM Asiakkaat WHERE nimi=?))");
            for (int i=1 ; i<=1000 ; i++) {
                p.setString(1,"k"+i);
                p.setString(2, "A"+i);
                p.executeUpdate();
            }
            loppu=System.nanoTime();
            
            System.out.println("Pakettien (1000 kpl) lisäämiseen kului: "+((loppu-alku)/1e9)+" s");
            
            
            //Tapahtumat
            alku=System.nanoTime();
            Random random = new Random();
            int luku;
            
            p=db.prepareStatement("INSERT INTO Tapahtumat (paketti_id, paikka_id, kuvaus, aika) "
                    + "VALUES ((SELECT id FROM Paketit WHERE seurantakoodi=?), "
                    + "(SELECT id FROM Paikat WHERE nimi=?), ?, DATETIME('now','localtime'))");
            
            for(int i=1 ; i<=1000000 ; i++) {
                luku=1+random.nextInt(1001);
                p.setString(1,"k"+luku);
                p.setString(2,"P"+luku);
                p.setString(3,"tapahtuma "+luku);
                p.executeUpdate();
            }
            loppu=System.nanoTime();
            System.out.println("Miljoonan tapahtumaan lisäämiseen kului: "+((loppu-alku)/1e9)+" s");
 
            //lopetetaan transaktio
            s.execute("COMMIT");
            
            //Tuhat kyselyä, haetaan jonkin asiakkaan pakettien määrä
            alku=System.nanoTime();
            for (int i=1 ; i<=1000 ; i++) {
                p=db.prepareStatement("SELECT COUNT(seurantakoodi) FROM Paketit WHERE asiakas_id=(SELECT id FROM Asiakkaat WHERE nimi=?)");
                p.setString(1, "A"+i);
                p.executeQuery();
            }
            loppu=System.nanoTime();
            System.out.println("Tuhannen asiakkaan pakettien hakemiseen kului: "+((loppu-alku)/1e9)+" s");
            
            
            //Tuhannen paketin tapahtumien määrän hakemiseen kuluva aika:
            alku=System.nanoTime();
            p=db.prepareStatement("SELECT COUNT(id) maara FROM Tapahtumat WHERE paketti_id=(SELECT id FROM Paketit WHERE seurantakoodi=?)");
            
            for (int i=1 ; i<=1000 ; i++) {
                p.setString(1,"k"+i);
                p.executeQuery();
            }
            loppu=System.nanoTime();
            System.out.println("Tuhannen paketin tapahtumien määrän hakemiseen kului: "+((loppu-alku)/1e9)+" s");
            
            
            
            
        } catch (SQLException e) {
            getErrorMessages(e);
        }
        
    }
    
    public void indeksoi() {
        try (Connection db = DriverManager.getConnection("jdbc:sqlite:"+this.nimi)) {
            Statement s = db.createStatement();
            s.execute("CREATE INDEX idx_paikka ON Paikat (nimi);");
            s.execute("CREATE INDEX idx_asiakas ON Asiakkaat (nimi);");
            s.execute("CREATE INDEX idx_paketti ON Paketit (seurantakoodi);");
            s.execute("CREATE INDEX idx_paketit_asiakas_id ON Paketit (asiakas_id);");
            s.execute("CREATE INDEX idx_tapahtumat_paketti_id ON Tapahtumat (paketti_id);");
            s.execute("CREATE INDEX idx_tapahtumat_paikka_id ON Tapahtumat (paikka_id);");
        } catch (SQLException e) {
            System.out.println("Virhe indeksien luomisessa.");
            getErrorMessages(e);
        }
    }
    
    //APUMETODIT:
    
    public int haeAsiakkaanId(String nimi) {
        try (Connection db = DriverManager.getConnection("jdbc:sqlite:"+this.nimi)) {
            PreparedStatement s = db.prepareStatement("SELECT id FROM Asiakkaat WHERE nimi=?");
            s.setString(1,nimi);
            ResultSet rs = s.executeQuery();
            int id=rs.getInt("id");
            return id;
        } catch (SQLException e) {
            return -1;
        }
    }
    
    public int haePaketinId(String koodi) {
        try (Connection db = DriverManager.getConnection("jdbc:sqlite:"+this.nimi)) {
            PreparedStatement s = db.prepareStatement("SELECT id FROM Paketit WHERE seurantakoodi=?");
            s.setString(1,koodi);
            ResultSet rs = s.executeQuery();
            int id=rs.getInt("id");
            return id;
        } catch (SQLException e) {
            return -1;
        }
    }
    
    public int haePaikanId(String nimi) {
        try (Connection db = DriverManager.getConnection("jdbc:sqlite:"+this.nimi)) {
            PreparedStatement s = db.prepareStatement("SELECT id FROM Paikat WHERE nimi=?");
            s.setString(1,nimi);
            ResultSet rs = s.executeQuery();
            int id=rs.getInt("id");
            return id;
        } catch (SQLException e) {
            return -1;
        }
    }
    
    public String haeAika() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        return dtf.format(now);
    }
    
    //metodi formatoiPaiva muuttaa muotoa d.m.yyyy olevan päivän muotoon yyyy-MM-dd
    public String formatoiPaivamaara(String paivamaara) {
        
        String[] palat = paivamaara.split("\\.");
        
        if (palat.length != 3) {
            System.out.println("Tarkista päivämäärä.");
        }
        
        palat[0]=lisaaNolla(palat[0]);
        palat[1]=lisaaNolla(palat[1]);
        
        String formatoituPaiva = palat[2]+"-"+palat[1]+"-"+palat[0];
        
        return formatoituPaiva;
    }
    
    //lisätään nollat pienten päivien ja kuukausien eteen mikäli käyttäjä ei niitä syöttänyt
    public String lisaaNolla(String aikapala) {
        int x = Integer.valueOf(aikapala);      //vaikka käyttäjä olisi syöttänyt 01, muuttuu se tässä vaiheessa muotoon 1
        String uusiAikapala = null;
        if (x<10) {
            uusiAikapala="0"+x;
            return uusiAikapala;
        }
        return aikapala;
    }
    
    
    public void tulostaTietokanta() {
        try (Connection db = DriverManager.getConnection("jdbc:sqlite:"+this.nimi)) {
        Statement s = db.createStatement();
        //Asiakkaat
        ResultSet r = s.executeQuery("SELECT * FROM Asiakkaat");
        System.out.println("ASIAKKAAT");
        while (r.next()) {
            System.out.println(r.getInt("id")+" "+r.getString("nimi"));
        }
        System.out.println("");
        //Paikat
        r=s.executeQuery("SELECT * FROM Paikat");
        System.out.println("PAIKAT");
        while (r.next()) {
            System.out.println(r.getInt("id")+" "+r.getString("nimi"));
        }
        System.out.println("");
        //Paketit
        r=s.executeQuery("SELECT * FROM Paketit");
        System.out.println("PAKETIT");
        while (r.next()) {
            System.out.println(r.getInt("id")+" "+r.getString("seurantakoodi")+" "+r.getString("asiakas_id"));
        }
        System.out.println("");
        //Tapahtumat
        r=s.executeQuery("SELECT * FROM Tapahtumat");
        System.out.println("TAPAHTUMAT");
        while (r.next()) {
            System.out.println(r.getInt("id")+" "+r.getString("paketti_id")+" "+r.getString("paikka_id")+" "+r.getString("kuvaus")+" "+r.getString("aika"));
        }
        System.out.println("");
        
        } catch (SQLException e) {
            System.out.println("Ongelma taulujen tulostuksessa.");
            getErrorMessages(e);
        }
    }
    
    //erillinen metodi virheviesteille
    public void getErrorMessages(SQLException e) {
        do {
            System.err.println("Viesti: " + e.getMessage());
            System.err.println("Virhekoodi: " + e.getErrorCode());
            System.err.println("SQL-tilakoodi: " + e.getSQLState());
        } while (e.getNextException() != null);
    }
}
