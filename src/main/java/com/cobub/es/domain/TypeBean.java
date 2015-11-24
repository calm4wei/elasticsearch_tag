package com.cobub.es.domain;

/**
 * Created by feng.wei on 2015/11/20.
 */
public class TypeBean {

    private String id;
    private String name;
    private String description;

    public TypeBean() {
    }

    public TypeBean(String id, String name) {
        this.id = id;
        this.name = name;
    }

    public TypeBean(String id, String name, String description) {
        this.id = id;
        this.name = name;
        this.description = description;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        return "TypeBean{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", description='" + description + '\'' +
                '}';
    }
}
