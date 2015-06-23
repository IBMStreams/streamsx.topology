package ThirdParty;

import java.io.Serializable;

public class ThirdPartyResource implements Serializable{
    private String thirdPartyString;

    public ThirdPartyResource(String val){
	thirdPartyString = val;
    }
    public static String thirdPartyStaticMethod(){
	return "This was returned from a third-party method";
    }

    public String thirdPartyMethod(){
	return thirdPartyString;
    }
}