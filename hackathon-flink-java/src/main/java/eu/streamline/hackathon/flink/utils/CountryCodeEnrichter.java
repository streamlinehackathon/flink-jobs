package eu.streamline.hackathon.flink.utils;



import java.io.BufferedReader;

import java.io.FileNotFoundException;

import java.io.FileReader;

import java.io.IOException;

import java.util.HashMap;

import java.util.Map;



public enum CountryCodeEnrichter {

    INSTANCE;



    //TODO

    public static final String FILE_LOCATION = "/home/virosagos/Scrivania/domaincountries.csv";

    private Map<String, String> url2country = null;





    public static void main(String[] args) {

        System.out.println(CountryCodeEnrichter.INSTANCE.getCountryCode("www.queenslandcountrylife.com.au"));

        System.out.println(CountryCodeEnrichter.INSTANCE.getCountryCode("www.southwalesguardian.co.uk"));

        System.out.println(CountryCodeEnrichter.INSTANCE.getCountryCode("adage.com"));

    }



    public String getCountryCode(String url) {

        // remove possible prefixes

        String shortenedUrl = url.replace("www.","");



        if (url2country == null) {

            loadUrl2Country();

        }



        if (url2country.containsKey(shortenedUrl)) {

            String countryCode = url2country.get(shortenedUrl);

            return changeCountryCode(countryCode);

        } else {

            return null;

        }

    }



    private void loadUrl2Country() {

        url2country = new HashMap<>();



        try (BufferedReader br = new BufferedReader(new FileReader(FILE_LOCATION))) {

            String line;

            while ((line = br.readLine()) != null) {

                String[] s = line.split(",");

                if (s[1] != "None") {

                    url2country.put(s[0], s[1]);

                }

            }

        } catch (FileNotFoundException e) {

            e.printStackTrace();

        } catch (IOException e) {

            e.printStackTrace();

        }

    }



    private String changeCountryCode(String code) {

        switch (code) {

            case "DE":

                return "DEU";

            case "NO":

                return "NOR";

            case "NP":

                return "NPL";

            case "RU":

                return "RUS";

            case "HK":

                return "CHN";

            case "BE":

                return "BEL";

            case "TW":

                return "TWN";

            case "BG":

                return "BGR";

            case "JP":

                return "JPN";

            case "DK":

                return "DNK";

            case "NZ":

                return "NZL";

            case "FR":

                return "FRA";

            case "QA":

                return "QAT";

            case "SE":

                return "SEN";

            case "SG":

                return "SVK";

            case "GB":

                return "GBR";

            case "ID":

                return "IND";

            case "IE":

                return "ISL";

            case "US":

                return "USA";

            case "CA":

                return "CAN";

            case "EG":

                return "EQY";

            case "MP":

                return "MYS";

            case "IL":

                return "ISL";

            case "AE":

                return "ARE";

            case "IN":

                return "IND";

            case "CH":

                return "CHN";

            case "ZA":

                return "ZAF";

            case "KR":

                return "KOR";

            case "SZ":

                return "SWZ";

            case "IR":

                return "IRN";

            case "IS":

                return "ISR";

            case "CN":

                return "CAN";

            case "MY":

                return "MYS";

            case "ES":

                return "ESP";

            case "AR":

                return "ARG";

            case "VG":

                return "VEN";

            case "AT":

                return "ATA";

            case "AU":

                return "AUT";

            case "TH":

                return "THA";

            case "LB":

                return "LBN";

            case "VN":

                return "VNM";

            case "AZ":

                return "AZE";

            case "PL":

                return "POL";

            case "NL":

                return "NLD";

            case "TR":

                return "TUR";

            case "LK":

                return "LKA";

            default:

                return null;

        }





    }





}