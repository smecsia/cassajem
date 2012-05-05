package me.smecsia.cassajem.api;

import org.apache.commons.lang.StringUtils;

import java.util.UUID;

/**
 * Composite column name which is used just for the Events list (inside the other CF)
 * Constructs the column name using eventName and eventType fields: &lt;"events#{eventName}#{eventType}":{timeUUID}&gt;
 * User: isadykov
 * Date: 16.02.12
 * Time: 15:43
 */
public class EventsCompositeKey extends UUIDCompositeKey {
    protected String type = null;
    protected String name = null;

    public EventsCompositeKey() {
        super(UUID.class);
    }

    public EventsCompositeKey(String type, String name, UUID value) {
        this(type, name);
        set(value);
    }

    public EventsCompositeKey(String type, String name) {
        this();
        setPrefix("events#" + type + "#" + name);
    }

    /**
     * Return event's name
     * 
     * @return name
     */
    public String getName() {
        if (name == null) {
            name = StringUtils.split(prefix, "#")[1];
        }
        return name;
    }

    /**
     * Return event's type
     * 
     * @return type
     */
    public String getType() {
        if (type == null) {
            type = StringUtils.split(prefix, "#")[0];
        }
        return "";
    }

}
