package com.afsun.lineage.controller.handler;

import com.afsun.lineage.core.exceptions.InternalParseException;
import com.afsun.lineage.core.exceptions.MetadataNotFoundException;
import com.afsun.lineage.core.exceptions.UnsupportedSyntaxException;
import com.afsun.lineage.vo.Response;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.multipart.MaxUploadSizeExceededException;

/**
 * 全局异常处理器
 * 统一处理SQL血缘解析过程中的各类异常，提供友好的错误响应
 *
 * @author afsun
 */
@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    /**
     * 处理元数据未找到异常
     */
    @ExceptionHandler(MetadataNotFoundException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Response<Void> handleMetadataNotFoundException(MetadataNotFoundException e) {
        log.warn("元数据未找到: {}", e.getMessage());
        return Response.fail(400, "元数据缺失: " + e.getMessage() +
                           "\n建议：1) 检查表名是否正确 2) 调用 /metadata/reload 刷新元数据");
    }

    /**
     * 处理不支持的SQL语法异常
     */
    @ExceptionHandler(UnsupportedSyntaxException.class)
    @ResponseStatus(HttpStatus.UNPROCESSABLE_ENTITY)
    public Response<Void> handleUnsupportedSyntaxException(UnsupportedSyntaxException e) {
        log.warn("不支持的SQL语法: {}", e.getMessage());
        return Response.fail(422, "不支持的SQL语法: " + e.getMessage() +
                           "\n建议：1) 简化SQL语句 2) 拆分为多个简单语句 3) 查看文档了解支持的语法");
    }

    /**
     * 处理内部解析异常
     */
    @ExceptionHandler(InternalParseException.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public Response<Void> handleInternalParseException(InternalParseException e) {
        log.error("SQL解析内部错误", e);
        return Response.fail(500, "解析失败: " + e.getMessage() +
                           "\n这是一个内部错误，请联系技术支持并提供完整的SQL脚本");
    }

    /**
     * 处理文件上传大小超限异常
     */
    @ExceptionHandler(MaxUploadSizeExceededException.class)
    @ResponseStatus(HttpStatus.PAYLOAD_TOO_LARGE)
    public Response<Void> handleMaxUploadSizeExceededException(MaxUploadSizeExceededException e) {
        log.warn("文件上传大小超限: {}", e.getMessage());
        return Response.fail(413, "文件大小超过限制，请上传较小的文件或使用文本接口");
    }

    /**
     * 处理非法参数异常
     */
    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Response<Void> handleIllegalArgumentException(IllegalArgumentException e) {
        log.warn("非法参数: {}", e.getMessage());
        return Response.fail(400, "参数错误: " + e.getMessage());
    }

    /**
     * 处理其他未预期异常
     */
    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public Response<Void> handleException(Exception e) {
        log.error("系统异常", e);
        return Response.fail(500, "系统错误: " + e.getMessage() +
                           "\n请联系技术支持");
    }
}
