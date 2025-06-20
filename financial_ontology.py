"""
Financial Document Ontology for Enhanced Data Extraction
"""

# Financial Document Types
DOCUMENT_TYPES = {
    'earnings_report': ['earnings', 'quarterly', 'annual', '10-k', '10-q', 'earnings call'],
    'financial_statement': ['balance sheet', 'income statement', 'cash flow', 'statement'],
    'investor_presentation': ['investor', 'presentation', 'deck', 'slides'],
    'sec_filing': ['sec', 'filing', '8-k', '10-k', '10-q', 'proxy'],
    'analyst_report': ['analyst', 'research', 'rating', 'recommendation'],
    'press_release': ['press release', 'announcement', 'news']
}

# Financial Metrics Taxonomy
FINANCIAL_METRICS = {
    'revenue': {
        'keywords': ['revenue', 'sales', 'net sales', 'total revenue', 'gross revenue', 'top line'],
        'units': ['million', 'billion', 'thousand', 'M', 'B', 'K'],
        'periods': ['quarterly', 'annual', 'ytd', 'q1', 'q2', 'q3', 'q4', 'fy'],
        'type': 'income'
    },
    'profit': {
        'keywords': ['profit', 'net income', 'earnings', 'ebitda', 'operating income', 'gross profit'],
        'units': ['million', 'billion', 'thousand', 'M', 'B', 'K'],
        'periods': ['quarterly', 'annual', 'ytd', 'q1', 'q2', 'q3', 'q4', 'fy'],
        'type': 'income'
    },
    'growth': {
        'keywords': ['growth', 'yoy', 'year-over-year', 'increase', 'growth rate', 'cagr'],
        'units': ['%', 'percent', 'percentage'],
        'periods': ['quarterly', 'annual', 'ytd'],
        'type': 'ratio'
    },
    'margins': {
        'keywords': ['margin', 'gross margin', 'operating margin', 'net margin', 'ebitda margin'],
        'units': ['%', 'percent', 'percentage'],
        'periods': ['quarterly', 'annual'],
        'type': 'ratio'
    },
    'users': {
        'keywords': ['users', 'mau', 'dau', 'subscribers', 'customers', 'active users'],
        'units': ['million', 'billion', 'thousand', 'M', 'B', 'K'],
        'periods': ['monthly', 'quarterly', 'annual'],
        'type': 'operational'
    },
    'market_cap': {
        'keywords': ['market cap', 'market capitalization', 'valuation', 'enterprise value'],
        'units': ['million', 'billion', 'thousand', 'M', 'B', 'K'],
        'periods': ['current', 'as of'],
        'type': 'valuation'
    }
}

# Financial Context Indicators
FINANCIAL_CONTEXT = {
    'positive_indicators': [
        'exceeded', 'outperformed', 'grew', 'increased', 'improved', 'strong', 
        'robust', 'solid', 'beat', 'above expectations', 'record', 'milestone'
    ],
    'negative_indicators': [
        'declined', 'decreased', 'fell', 'dropped', 'weak', 'disappointing',
        'below expectations', 'missed', 'lower', 'reduced', 'loss'
    ],
    'neutral_indicators': [
        'reported', 'announced', 'stated', 'disclosed', 'maintained', 'stable'
    ]
}

# Financial Periods and Timeframes
FINANCIAL_PERIODS = {
    'quarters': ['q1', 'q2', 'q3', 'q4', 'first quarter', 'second quarter', 'third quarter', 'fourth quarter'],
    'years': ['fy', 'fiscal year', 'calendar year', '2023', '2024', '2025'],
    'comparative': ['yoy', 'year-over-year', 'sequential', 'vs prior year', 'compared to']
}

# Currency and Unit Patterns
CURRENCY_PATTERNS = {
    'symbols': ['$', '€', '£', '¥', '₹'],
    'words': ['dollar', 'euro', 'pound', 'yen', 'rupee', 'usd', 'eur', 'gbp'],
    'units': ['million', 'billion', 'trillion', 'thousand', 'M', 'B', 'T', 'K']
}

def classify_financial_metric(text):
    """Classify text as a specific type of financial metric"""
    text_lower = text.lower()
    
    for metric_type, config in FINANCIAL_METRICS.items():
        if any(keyword in text_lower for keyword in config['keywords']):
            return {
                'type': metric_type,
                'category': config['type'],
                'confidence': 0.8
            }
    
    return {'type': 'other', 'category': 'unknown', 'confidence': 0.1}

def extract_financial_context(text):
    """Extract sentiment and context from financial text"""
    text_lower = text.lower()
    
    positive_count = sum(1 for indicator in FINANCIAL_CONTEXT['positive_indicators'] if indicator in text_lower)
    negative_count = sum(1 for indicator in FINANCIAL_CONTEXT['negative_indicators'] if indicator in text_lower)
    
    if positive_count > negative_count:
        return 'positive'
    elif negative_count > positive_count:
        return 'negative'
    else:
        return 'neutral'

def identify_financial_period(text):
    """Identify financial reporting periods in text"""
    text_lower = text.lower()
    
    for period_type, patterns in FINANCIAL_PERIODS.items():
        for pattern in patterns:
            if pattern in text_lower:
                return {
                    'period_type': period_type,
                    'pattern': pattern
                }
    
    return None

def detect_currency_and_units(text):
    """Detect currency symbols and units in financial data"""
    detected = {
        'currency': None,
        'units': [],
        'has_financial_data': False
    }
    
    # Check for currency symbols
    for symbol in CURRENCY_PATTERNS['symbols']:
        if symbol in text:
            detected['currency'] = symbol
            detected['has_financial_data'] = True
    
    # Check for currency words
    text_lower = text.lower()
    for word in CURRENCY_PATTERNS['words']:
        if word in text_lower:
            detected['currency'] = word
            detected['has_financial_data'] = True
    
    # Check for units
    for unit in CURRENCY_PATTERNS['units']:
        if unit in text:
            detected['units'].append(unit)
            detected['has_financial_data'] = True
    
    return detected