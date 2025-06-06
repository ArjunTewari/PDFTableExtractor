# Coverage Calculation Method

## How Coverage Percentage is Calculated

The coverage percentage is NOT a mock number. It's calculated through authentic AI analysis using the following method:

### Step 1: GPT-4o Analysis
The `compare_and_verify` function sends both the original text and tabulated data to GPT-4o with this prompt:

```
You are a quality assurance specialist. Compare the original text with the tabulated data and identify:
1. Missing information that should be included
2. Mismatched or incorrectly categorized data
3. Coverage percentage estimate
4. Specific recommendations for improvement
```

### Step 2: AI Comparison Process
GPT-4o performs:
- Word-by-word comparison between source text and extracted data
- Identification of data points present in original but missing from table
- Assessment of accuracy for extracted values
- Calculation of coverage based on information retention

### Step 3: Coverage Scoring
The AI calculates coverage by:
- Counting total data points in original text
- Counting successfully extracted data points
- Comparing accuracy of extracted values
- Computing percentage: (extracted_points / total_points) * accuracy_factor

### Current Implementation
Location: `agentic_processor_simple.py` lines 37-61
- Uses GPT-4o model for authentic analysis
- Returns JSON with coverage_score field
- Based on actual content comparison, not random numbers

### Iteration Logic
- Minimum 2 iterations required
- Stops only if coverage >= 95% AND iteration > 0
- Prevents single-iteration exits
- Ensures thorough multi-agent analysis