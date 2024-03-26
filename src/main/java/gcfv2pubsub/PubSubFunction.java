package gcfv2pubsub;

import com.google.cloud.functions.CloudEventsFunction;
import com.google.events.cloud.pubsub.v1.Message;
import com.google.events.cloud.pubsub.v1.MessagePublishedData;
import com.google.gson.Gson;
import com.mailgun.api.v3.MailgunMessagesApi;
import com.mailgun.client.MailgunClient;
import com.mailgun.model.message.MessageResponse;
import io.cloudevents.CloudEvent;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.logging.Logger;

public class PubSubFunction implements CloudEventsFunction {

    private static final Logger logger = Logger.getLogger(PubSubFunction.class.getName());

    static final String DB_URL = "jdbc:mysql://10.45.203.2:3306/webapp?createDatabaseIfNotExist=true";
    static final String USER = "webapp";
    static final String PASS = "N+]KMyLn=N#<lvzk";
    static final String QUERY = "SELECT * FROM user";

    @Override
    public void accept(CloudEvent event) {


        logger.info("**** Inside The Cloud Function ****");

        String cloudEventData = new String(event.getData().toBytes());
        Gson gson = new Gson();
        MessagePublishedData data = gson.fromJson(cloudEventData, MessagePublishedData.class);
        Message message = data.getMessage();
        String encodedData = message.getData();
        String queryParams = new String(Base64.getDecoder().decode(encodedData));
        String senderEmail = queryParams.split("&")[0];
        logger.info("Pub/Sub message: " + senderEmail);

        String DOMAIN = System.getenv("DOMAIN");
        String PRIVATE_API_KEY = System.getenv("PRIVATE_API_KEY");
        logger.info("PRIVATE_API_KEY: " + PRIVATE_API_KEY);
        logger.info("DOMAIN: " + DOMAIN);
        MailgunMessagesApi mailgunMessagesApi = MailgunClient.config(PRIVATE_API_KEY)
                .createApi(MailgunMessagesApi.class);
        String verificationLink = "http://sharankumar.me:8080/v1/user/verification?" + queryParams;
        com.mailgun.model.message.Message emailMessage = com.mailgun.model.message.Message.builder()
                .from("noreply@mail.sharakumar.me")
                .to(senderEmail)
                .subject("User Account Verification Email")
                .text("This is a User Account Verification Email. Please click the link within 2 mins, to verify your account. \n " + verificationLink)
                .build();
        MessageResponse messageResponse = mailgunMessagesApi.sendMessage(DOMAIN, emailMessage);
        updateUserData(senderEmail);

    }

    private void updateUserData (String username){
        logger.info("**** Inside updateUserData ****");
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
            Statement stmt = conn.createStatement();
            String query = "UPDATE user SET email_sent_time = '" + LocalDateTime.now(ZoneOffset.UTC) + "' WHERE username = '" + username + "'";
            stmt.executeUpdate(query);
            ResultSet rs = stmt.executeQuery(QUERY);
            while(rs.next()){
                logger.info("email_sent_time: " + rs.getString("email_sent_time"));
            }
            rs.close();
        } catch (SQLException | ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }

    }

}

