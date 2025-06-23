public class GlobalExceptionHandler {

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidationExceptions(MethodArgumentNotValidException ex) {
        ErrorResponse errorResponse = new ErrorResponse("Validation failed", ex.getBindingResult().toString());
        return ResponseEntity.badRequest().body(errorResponse);
    }
}