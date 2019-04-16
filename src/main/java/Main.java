import org.apache.hadoop.io.Text;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws ParseException, org.json.simple.parser.ParseException {
//        String timestamp = "2015-07-22T09:00:28.019143Z";
//        Date date = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'").parse(timestamp);
//        System.out.println(date);

        String input = "2015-07-22T09:00:28.019143Z marketpalce-shop 123.242.248.130:54635 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 \"GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2";
        int first_end = input.indexOf("\"");
        String firstPart = input.substring(0, first_end);
        String[] components = firstPart.toString().split(" ");
        Map<String, Object> properties = new HashMap<>();
        String result = "";
        try {
            Date timestamp = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'").parse(components[0]);
            result += "timestamp=" + String.valueOf(timestamp.getTime());
            properties.put("timestamp", timestamp.getTime());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        String ip = components[2].substring(0, components[2].indexOf(':'));
        result += ";ip=" + ip;
        properties.put("ip", ip);
        int url_end = input.indexOf("\"", first_end + 1);
        System.out.println(first_end + ", " + url_end);
        System.out.println(input.substring(first_end, url_end));
        String page = input.substring(first_end, url_end).split(" ")[1];
        result += ";page=" + page;
        properties.put("page", page);
        System.out.println(properties);
        System.out.println(result);
        String after = "{\"ip\":\"1.186.101.79\",\"page\":\"\\\"https:\\/\\/paytm.com:443\\/papi\\/v1\\/expresscart\\/verify\\\"\",\"timestamp\":\"1437562607943\"}";
        JSONObject jsonObject = (JSONObject)new JSONParser().parse(after);
        System.out.println(jsonObject.get("page").toString());
//        context.write(new Text(ip), new Text(jsonObject.toJSONString()));
    }
}
