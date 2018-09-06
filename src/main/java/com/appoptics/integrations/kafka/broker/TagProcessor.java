package com.appoptics.integrations.kafka.broker;

import com.librato.metrics.client.Tag;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
/**
 * A Tag Processor to check the APPOPTICS_TAGS passed in meet criteria located @ https://docs.appoptics.com/api/#measurement-restrictions
 */
final class TagProcessor {
    private static final String TAG_NAME_REGEX = "\\A[-.:_?\\\\/\\w ]{1,255}\\z";
    private static final String TAG_VALUE_REGEX = "\\A[-.:_\\w]{1,64}\\z";
    private static final String TAG_KV_SEPARATOR = "=";
    private static final String TAGS_SEPARATOR = ",";

    /**
     * Converts a string of tags into a List of Tags after validating
     * @param customTags string of Key=Value pairs of custom tags to be attached to the metric
     * @return List of Tag objects
     */
    static List<Tag> process(String customTags){
        String[] rawTags = new String[]{customTags};
        List<Tag> tags = new ArrayList<>();
        if (customTags.contains(TAGS_SEPARATOR)) {
            rawTags = customTags.split(TAGS_SEPARATOR);
        }
        for (String tagString : rawTags) {
            if (customTags.contains(TAG_KV_SEPARATOR)) {
                String tagProperties[] = tagString.split(TAG_KV_SEPARATOR);
                if (tagProperties.length == 2) {
                    if(isTagValid(tagProperties[0],tagProperties[1])){
                        Tag tag = new Tag(tagProperties[0], tagProperties[1]);
                        tags.add(tag);
                    }
                }
            }
        }
        return tags;
    }

    /**
     * Ensures the name & key pass the regex criteria
     * @param name the Key for the metric tag
     * @param value the Value for the metric tag
     * @return boolean True if it meets the REGEX Requirements, False if either the name or the value fail to match
     */
    private static boolean isTagValid(String name, String value){
        Pattern r = Pattern.compile(TAG_NAME_REGEX);
        Matcher m = r.matcher(name);
        if (!m.find()) {
            return false;
        }
        r = Pattern.compile(TAG_VALUE_REGEX);
        m = r.matcher(value);
        return m.find();
    }
}


