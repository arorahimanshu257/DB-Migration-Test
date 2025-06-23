public class ExampleController {

    @Autowired
    private ExampleService exampleService;

    @GetMapping("/example")
    public ResponseEntity<ExampleResponse> getExample() {
        ExampleResponse response = exampleService.getExample();
        return ResponseEntity.ok(response);
    }

    @PostMapping("/example")
    public ResponseEntity<ExampleResponse> createExample(@Valid @RequestBody ExampleRequest request) {
        ExampleResponse response = exampleService.createExample(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }
}