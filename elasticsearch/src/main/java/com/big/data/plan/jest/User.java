package com.big.data.plan.jest;

import io.searchbox.annotations.JestId;

/**
  * @author huangfox
  * @date 2014年1月22日 下午5:31:52
  *
  */
public class User {
    @JestId
    private Long id;
    private String name;
    private int age;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }
}
