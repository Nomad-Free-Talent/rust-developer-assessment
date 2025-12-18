// Example implementation of XML processing functionality
// This is a minimal working example - candidates should implement their own optimized version

use crate::part2_xml::*;

pub struct ExampleHotelSearchProcessor;

impl Default for ExampleHotelSearchProcessor {
    fn default() -> Self {
        Self::new()
    }
}

impl ExampleHotelSearchProcessor {
    pub fn new() -> Self {
        Self
    }

    // Basic XML processing implementation using simple string parsing
    // Note: This is a simplified example - real implementation should use proper XML parsing
    pub fn process(&self, xml: &str) -> Result<ProcessedResponse, ProcessingError> {
        let mut hotels = Vec::new();

        // Simple regex-like parsing for the example
        // Extract hotel code and name
        if let Some(hotel_start) = xml.find(r#"<Hotel code=""#) {
            let hotel_section = &xml[hotel_start..];

            // Extract hotel code
            let hotel_code = if let Some(code_start) = hotel_section.find(r#"code=""#) {
                let code_start = code_start + 6; // Skip 'code="'
                if let Some(code_end) = hotel_section[code_start..].find('"') {
                    hotel_section[code_start..code_start + code_end].to_string()
                } else {
                    "unknown".to_string()
                }
            } else {
                "unknown".to_string()
            };

            // Extract hotel name
            let hotel_name = if let Some(name_start) = hotel_section.find(r#"name=""#) {
                let name_start = name_start + 6; // Skip 'name="'
                if let Some(name_end) = hotel_section[name_start..].find('"') {
                    hotel_section[name_start..name_start + name_end].to_string()
                } else {
                    "Unknown Hotel".to_string()
                }
            } else {
                "Unknown Hotel".to_string()
            };

            // Extract board type (meal plan code) - handle whitespace
            let board_type = if let Some(meal_start) = hotel_section.find("MealPlan") {
                let meal_section = &hotel_section[meal_start..];
                if let Some(code_start) = meal_section.find(r#"code=""#) {
                    let code_start = code_start + 6; // Skip 'code="'
                    if let Some(code_end) = meal_section[code_start..].find('"') {
                        meal_section[code_start..code_start + code_end].to_string()
                    } else {
                        "RO".to_string()
                    }
                } else {
                    "RO".to_string()
                }
            } else {
                "RO".to_string()
            };

            // Extract price and currency
            let (price_amount, price_currency) =
                if let Some(price_start) = hotel_section.find(r#"<Price currency=""#) {
                    let price_section = &hotel_section[price_start..];

                    // Extract currency
                    let currency = if let Some(curr_start) = price_section.find(r#"currency=""#) {
                        let curr_start = curr_start + 10; // Skip 'currency="'
                        if let Some(curr_end) = price_section[curr_start..].find('"') {
                            price_section[curr_start..curr_start + curr_end].to_string()
                        } else {
                            "USD".to_string()
                        }
                    } else {
                        "USD".to_string()
                    };

                    // Extract amount
                    let amount = if let Some(amt_start) = price_section.find(r#"amount=""#) {
                        let amt_start = amt_start + 8; // Skip 'amount="'
                        if let Some(amt_end) = price_section[amt_start..].find('"') {
                            price_section[amt_start..amt_start + amt_end]
                                .parse::<f64>()
                                .unwrap_or(0.0)
                        } else {
                            0.0
                        }
                    } else {
                        0.0
                    };

                    (amount, currency)
                } else {
                    (0.0, "USD".to_string())
                };

            // Extract room description
            let room_description = if let Some(room_start) = hotel_section.find(r#"description=""#)
            {
                let room_start = room_start + 13; // Skip 'description="'
                if let Some(room_end) = hotel_section[room_start..].find('"') {
                    hotel_section[room_start..room_start + room_end].to_string()
                } else {
                    "Standard Room".to_string()
                }
            } else {
                "Standard Room".to_string()
            };

            // Extract search token
            let search_token =
                if let Some(token_start) = hotel_section.find(r#"key="search_token" value=""#) {
                    let token_start = token_start + 25; // Skip 'key="search_token" value="'
                    if let Some(token_end) = hotel_section[token_start..].find('"') {
                        hotel_section[token_start..token_start + token_end].to_string()
                    } else {
                        "default_token".to_string()
                    }
                } else {
                    "default_token".to_string()
                };

            // Check if refundable (simple check)
            let is_refundable = !hotel_section.contains(r#"nonRefundable="true""#);

            let hotel_option = HotelOption {
                hotel_id: hotel_code,
                hotel_name,
                room_type: room_description.clone(),
                room_description,
                board_type,
                price: Price {
                    amount: price_amount,
                    currency: price_currency.clone(),
                },
                cancellation_policies: vec![], // Simplified - real implementation would parse these
                payment_type: "MerchantPay".to_string(),
                is_refundable,
                search_token,
            };

            hotels.push(hotel_option);
        }

        Ok(ProcessedResponse {
            search_id: "example_search".to_string(),
            total_options: hotels.len(),
            hotels,
            currency: "GBP".to_string(), // Default from the sample
            nationality: "US".to_string(),
            check_in: "2025-06-11".to_string(),
            check_out: "2025-06-12".to_string(),
        })
    }

    // Basic XML to ProcessedResponse conversion
    pub fn xml_to_processed_response(
        &self,
        xml: &str,
    ) -> Result<ProcessedResponse, ProcessingError> {
        self.process(xml)
    }

    // Basic search parameter extraction using simple string parsing
    pub fn extract_search_params(
        &self,
        request_xml: &str,
    ) -> Result<(String, String, String, String), ProcessingError> {
        let mut currency = String::new();
        let mut nationality = String::new();
        let mut start_date = String::new();
        let mut end_date = String::new();

        // Extract currency
        if let Some(curr_start) = request_xml.find("<Currency>") {
            let curr_start = curr_start + 10; // Skip '<Currency>'
            if let Some(curr_end) = request_xml[curr_start..].find("</Currency>") {
                currency = request_xml[curr_start..curr_start + curr_end].to_string();
            }
        }

        // Extract nationality
        if let Some(nat_start) = request_xml.find("<Nationality>") {
            let nat_start = nat_start + 13; // Skip '<Nationality>'
            if let Some(nat_end) = request_xml[nat_start..].find("</Nationality>") {
                nationality = request_xml[nat_start..nat_start + nat_end].to_string();
            }
        }

        // Extract start date
        if let Some(start_pos) = request_xml.find("<StartDate>") {
            let start_pos = start_pos + 11; // Skip '<StartDate>'
            if let Some(end_pos) = request_xml[start_pos..].find("</StartDate>") {
                start_date = request_xml[start_pos..start_pos + end_pos].to_string();
            }
        }

        // Extract end date
        if let Some(start_pos) = request_xml.find("<EndDate>") {
            let start_pos = start_pos + 9; // Skip '<EndDate>'
            if let Some(end_pos) = request_xml[start_pos..].find("</EndDate>") {
                end_date = request_xml[start_pos..start_pos + end_pos].to_string();
            }
        }

        Ok((currency, nationality, start_date, end_date))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::part2_xml::SMALL_SAMPLE_XML;

    #[test]
    fn test_example_xml_processing() {
        let processor = ExampleHotelSearchProcessor::new();
        let result = processor.process(SMALL_SAMPLE_XML);

        assert!(result.is_ok());
        let response = result.unwrap();

        // Check basic response properties
        assert_eq!(response.hotels.len(), 1);

        // Check first hotel
        let hotel = &response.hotels[0];
        assert_eq!(hotel.hotel_id, "39776757");
        assert_eq!(hotel.hotel_name, "Days Inn By Wyndham Fargo");
        assert_eq!(hotel.board_type, "RO");
        assert_eq!(hotel.price.amount, 84.82);
        assert_eq!(hotel.price.currency, "GBP");
    }

    #[test]
    fn test_example_search_param_extraction() {
        let processor = ExampleHotelSearchProcessor::new();

        // Simple XML for testing
        let request_xml = r#"
        <AvailRQ>
            <Currency>GBP</Currency>
            <Nationality>US</Nationality>
            <StartDate>11/06/2025</StartDate>
            <EndDate>12/06/2025</EndDate>
        </AvailRQ>
        "#;

        let result = processor.extract_search_params(request_xml);
        assert!(result.is_ok());

        let (currency, nationality, start_date, end_date) = result.unwrap();
        assert_eq!(currency, "GBP");
        assert_eq!(nationality, "US");
        assert_eq!(start_date, "11/06/2025");
        assert_eq!(end_date, "12/06/2025");
    }
}
