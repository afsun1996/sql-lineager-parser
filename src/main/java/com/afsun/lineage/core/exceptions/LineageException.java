package com.afsun.lineage.core.exceptions;

import lombok.Getter;

/**
 * SQL血缘解析异常基类
 * 提供统一的异常处理和错误信息格式
 *
 * @author afsun
 */
@Getter
public class LineageException extends RuntimeException {

    /**
     * 错误码
     */
    private final String errorCode;

    /**
     * 错误详情
     */
    private final String errorDetail;

    /**
     * 建议解决方案
     */
    private final String suggestion;

    /**
     * SQL片段（可选）
     */
    private final String sqlFragment;

    public LineageException(String message) {
        super(message);
        this.errorCode = "LINEAGE_ERROR";
        this.errorDetail = message;
        this.suggestion = null;
        this.sqlFragment = null;
    }

    public LineageException(String message, Throwable cause) {
        super(message, cause);
        this.errorCode = "LINEAGE_ERROR";
        this.errorDetail = message;
        this.suggestion = null;
        this.sqlFragment = null;
    }

    public LineageException(String errorCode, String message, String suggestion) {
        super(message);
        this.errorCode = errorCode;
        this.errorDetail = message;
        this.suggestion = suggestion;
        this.sqlFragment = null;
    }

    public LineageException(String errorCode, String message, String suggestion, String sqlFragment) {
        super(message);
        this.errorCode = errorCode;
        this.errorDetail = message;
        this.suggestion = suggestion;
        this.sqlFragment = sqlFragment;
    }

    public LineageException(String errorCode, String message, String suggestion, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.errorDetail = message;
        this.suggestion = suggestion;
        this.sqlFragment = null;
    }

    /**
     * 获取格式化的错误信息
     */
    public String getFormattedMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append(errorCode).append("] ").append(errorDetail);

        if (sqlFragment != null && !sqlFragment.isEmpty()) {
            sb.append("\nSQL片段: ").append(sqlFragment);
        }

        if (suggestion != null && !suggestion.isEmpty()) {
            sb.append("\n建议: ").append(suggestion);
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return getFormattedMessage();
    }
}
