package json;

import org.codehaus.jackson.map.ObjectMapper;

import java.util.StringTokenizer;

/**
 * Created by minal on 18/07/17.
 */
public class Contact {
    private int contactId;
    private  String  firstName;
    private String  lastName;

    public Contact(){}

    public Contact(int contactId, String firstName, String lastName){
        this.contactId = contactId;
        this.firstName = firstName;
        this.lastName = lastName;
    }

    public void parseString(String csvStr) {
        StringTokenizer st = new StringTokenizer(csvStr, ",");
        contactId = Integer.parseInt(st.nextToken());
        firstName = st.nextToken();
        lastName = st.nextToken();
    }

    public int getContactId(){
        return contactId;
    }

    public void setContactId(int contactId){
        this.contactId = contactId;
    }

    public String getFirstName(){
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }
    public String  getLastName(){
        return lastName;
    }

    public void setLastName(String lastName){
        this.lastName = lastName;
    }


    public String toString(){
        return "Contact{" + "contactId=" + contactId + ", firstName='" + firstName + '\'' + ", lastName='"
                + lastName + '\'' + '}';
    }

    public static void main(String[] args)throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        Contact contact = new Contact();
        contact.setContactId(1);
        contact.setFirstName("Sachin");
        contact.setLastName("Tendulkar");
        System.out.println(mapper.writeValueAsString(contact));
        contact.parseString("1,Rahul,Dravid");
        System.out.println(mapper.writeValueAsString(contact));

    }




}
