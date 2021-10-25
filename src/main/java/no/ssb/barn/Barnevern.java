package no.ssb.barn;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import java.io.Serializable;

@XmlRootElement(name = "Barnevern")
public class Barnevern implements Serializable {

    private String navn;
    @XmlElement(name = "Navn")
    public String getNavn ()
    {
        return navn;
    }

    public void setNavn (String navn)
    {
        this.navn = navn;
    }

    private String versjon;
    @XmlElement(name = "Versjon")
    public String getVersjon ()
    {
        return versjon;
    }

    public void setVersjon (String versjon)
    {
        this.versjon = versjon;
    }

    @Override
    public String toString()
    {
        return "ClassPojo [name= "+navn+"]";
    }

}

