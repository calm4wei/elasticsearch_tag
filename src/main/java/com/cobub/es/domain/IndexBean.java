package com.cobub.es.domain;

import java.util.List;

/**
 * Created by feng.wei on 2015/11/20.
 */
public class IndexBean {

    private String id;
    private String name;
    private String description;
    private List<TypeBean> typeBeans;

    public IndexBean() {
    }

    public IndexBean(String id, String name) {
        this.id = id;
        this.name = name;
    }

    public IndexBean(String id, String name, String description, List<TypeBean> typeBeans) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.typeBeans = typeBeans;
    }

    public List<TypeBean> getTypeBeans() {
        return typeBeans;
    }

    public void setTypeBeans(List<TypeBean> typeBeans) {
        this.typeBeans = typeBeans;
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
        return "IndexBean{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", description='" + description + '\'' +
                '}';
    }
}
