package com.appoptics.integrations.kafka.broker;

import com.appoptics.metrics.client.Sanitizer;
import com.appoptics.metrics.client.Tag;

import java.util.ArrayList;
import java.util.List;
/**
 * A Tag Processor to check the APPOPTICS_TAGS passed in meet criteria located @ https://docs.appoptics.com/api/#measurement-restrictions
 */
final class TagProcessor {
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
                String[] tagProperties = tagString.split(TAG_KV_SEPARATOR);
                if (tagProperties.length == 2) {
                    String name = Sanitizer.TAG_NAME_SANITIZER.apply(tagProperties[0]);
                    String value = Sanitizer.TAG_VALUE_SANITIZER.apply(tagProperties[1]);
                    if(isTagValid(name, value)){
                        Tag tag = new Tag(tagProperties[0], tagProperties[1]);
                        tags.add(tag);
                    }
                }
            }
        }
        return tags;
    }

    /**
     * Ensures the name & key pass the non-empty criteria
     * @param name the Key for the metric tag
     * @param value the Value for the metric tag
     * @return boolean True if it meets the criteria
     */
    private static boolean isTagValid(String name, String value){
        return name != null && name.length() > 0 && value != null && value.length() > 0;
    }
}


