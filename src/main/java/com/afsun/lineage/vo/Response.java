package com.afsun.lineage.vo;


import lombok.Data;

/**
 * 响应对象
 */
@Data
public class Response<T> {

    private String status;

    private T data;

    private String message;

    /**
     * 操作成功状态码
     */
    public static final String SUCCESS = "200";

    /**
     * 操作失败状态码
     */
    public static final String FAIL = "500";

    public Response() {
    }

    public Response(String status) {
        this.data = null;
        this.status = status;
        this.message = "";
    }

    public Response(String status, T data) {
        this.status = status;
        this.data = data;
        this.message = "";
    }

    public Response(String status, String message) {
        this.status = status;
        this.data = null;
        this.message = message;
    }

    public Response(String status, T data, String message) {
        this.status = status;
        this.data = data;
        this.message = message;
    }

    public Response<T> setStatus(String status) {
        this.status = status;
        return this;
    }

    public Response<T> setData(T data) {
        this.data = data;
        return this;
    }

    public Response<T> setMessage(String message) {
        this.message = message;
        return this;
    }

    public static <T> boolean isValid(Response<T> res) {
        return res != null && SUCCESS.equals(res.getStatus()) && res.getData() != null;
    }

    public boolean isSuccess() {
        return SUCCESS.equals(getStatus());
    }


    /**
     * 操作成功返回响应
     *
     * @return 响应结果
     */
    public static <T> Response<T> success() {
        return new Response<>(SUCCESS);
    }

    /**
     * 操作成功返回响应
     *
     * @param data 返回数据
     * @return 响应结果
     */
    public static <T> Response<T> success(T data) {
        return new Response<>(SUCCESS, data);
    }

    /**
     * 操作失败返回响应
     *
     * @param message 错误信息
     * @return 响应结果
     */
    public static <T> Response<T> fail(String message) {
        return new Response<>(FAIL, message);
    }

    /**
     * 操作失败返回响应，可自定义状态码
     *
     * @param status  状态码
     * @param message 错误信息
     * @return 响应结果
     */
    public static <T> Response<T> fail(String status, String message) {
        return new Response<>(status, message);
    }

    /**
     * 操作失败返回响应，可自定义状态码
     *
     * @param status  状态码
     * @param message 错误信息
     * @param data    返回数据
     * @return 响应结果
     */
    public static <T> Response<T> fail(String status, String message, T data) {
        return new Response<>(status, data, message);
    }

    /**
     * 操作失败返回响应，可自定义状态码
     *
     * @param message 错误信息
     * @param data    返回数据
     * @return 响应结果
     */
    public static <T> Response<T> fail(String message, T data) {
        return new Response<>(FAIL, data, message);
    }

    /**
     * 操作失败返回响应，支持整数状态码
     *
     * @param statusCode 状态码
     * @param message    错误信息
     * @return 响应结果
     */
    public static <T> Response<T> fail(int statusCode, String message) {
        return new Response<>(String.valueOf(statusCode), message);
    }

}

