public class ThirdPartyResource{
    private String thirdPartyString;

    ThirdPartyResource(String val){
	thirdPartyString = val;
    }
    public static String thirdPartyStaticMethod(){
	return "This was returned from a third-party method";
    }

    public String thirdPartyMethod(){
	return thirdPartyString;
    }
}