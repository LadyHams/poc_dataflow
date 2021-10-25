package no.ssb.barn.validation;

import java.util.Date;

public class BarnValidationController {

    // skal vi logge exception her eller vi skal styre det i main func
    public Boolean ValidateDato(Date Start, Date Slutt) {
        int compareVal = Start.compareTo(Slutt);
        return (compareVal >= 0 ) ? true : false ;
    }

    // sjekke gyldig kode med kodeNavn
    public Boolean validateKode(String kodeNavn, String kodeVerdi) {
        switch(kodeNavn)
        {
            case "a":
                System.out.println("a");
                break;
            case "b":
                System.out.println("a");
                break;
            case "c":
                System.out.println("a");
                break;
            default:
                return false;
        }
        return false;
    }

    // sjekk kode lengde og gyldig kode med Enum verdier - potensielt
    public Boolean sjekkLengde( String nummerType, String nummer ) {
        switch (nummerType)
        {
            case "fnr":
                return (nummer.length() == 11) ? true : false;
            case "duf":
                return (nummer.length() == 12) ? true : false;
            case "kommune":
                return (nummer.length() == 4) ? true : false;
            default:
                return false;
        }
    }


}
