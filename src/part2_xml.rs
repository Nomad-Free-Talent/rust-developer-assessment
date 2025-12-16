// Part 2: XML Processing Implementation

use thiserror::Error;
use serde::{Deserialize, Serialize};

// Error types for XML processing
#[derive(Error, Debug)]
pub enum ProcessingError {
    #[error("XML parse error: {0}")]
    XmlParseError(String),
    
    #[error("JSON parse error: {0}")]
    JsonParseError(String),
    
    #[error("Missing required field: {0}")]
    MissingRequiredField(String),
    
    #[error("Invalid format: {0}")]
    InvalidFormat(String),
    
    #[error("Conversion error: {0}")]
    ConversionError(String),
    
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
    
    // Add other error types as needed
    #[error("Other error: {0}")]
    Other(String),
}

// Data structures for supplier JSON response
#[derive(Debug, Deserialize, Serialize)]
pub struct SupplierResponse {
    pub hotels: Vec<SupplierHotel>,
    pub search_id: String,
    pub currency: String,
    pub timestamp: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SupplierHotel {
    pub hotel_id: String,
    pub name: String,
    pub category: i32,
    pub rooms: Vec<SupplierRoom>,
    pub destination_code: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SupplierRoom {
    pub room_id: String,
    pub name: String,
    pub rates: Vec<SupplierRate>,
    pub capacity: RoomCapacity,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RoomCapacity {
    pub adults: i32,
    pub children: i32,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SupplierRate {
    pub rate_id: String,
    pub board_type: String,
    pub price: f64,
    pub cancellation_policies: Vec<SupplierCancellationPolicy>,
    pub booking_code: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SupplierCancellationPolicy {
    pub from_date: String,
    pub amount: f64,
}

// Data structures for XML response
#[derive(Debug)]
pub struct HotelAvailabilityResponse {
    pub hotels: Vec<HotelAvailability>,
    pub search_id: String,
    pub currency: String,
    pub timestamp: String,
}

#[derive(Debug)]
pub struct HotelAvailability {
    pub hotel_id: String,
    pub name: String,
    pub category: i32,
    pub room_types: Vec<RoomType>,
    pub destination_code: String,
}

#[derive(Debug)]
pub struct RoomType {
    pub code: String,
    pub name: String,
    pub rates: Vec<Rate>,
    pub capacity: RoomCapacity,
}

#[derive(Debug)]
pub struct Rate {
    pub rate_key: String,
    pub board_type: String,
    pub price: f64,
    pub currency: String,
    pub cancellation_policies: Vec<SupplierCancellationPolicy>,
    pub booking_code: String,
}

// Structures for hotel data
#[derive(Debug, Clone)]
pub struct ProcessedResponse {
    pub search_id: String,
    pub total_options: usize,
    pub hotels: Vec<HotelOption>,
    pub currency: String,
    pub nationality: String,
    pub check_in: String,
    pub check_out: String
}

#[derive(Debug, Clone)]
pub struct HotelOption {
    pub hotel_id: String,
    pub hotel_name: String,
    pub room_type: String,
    pub room_description: String,
    pub board_type: String,
    pub price: Price,
    pub cancellation_policies: Vec<ProcessedCancellationPolicy>,
    pub payment_type: String,
    pub is_refundable: bool,
    pub search_token: String
}

#[derive(Debug, Clone)]
pub struct Price {
    pub amount: f64,
    pub currency: String,
}

#[derive(Debug, Clone)]
pub struct ProcessedCancellationPolicy {
    pub deadline: String, // ISO date format
    pub penalty_amount: f64,
    pub currency: String,
    pub hours_before: i32,
    pub penalty_type: String, // "Importe" or "Porcentaje"
}

#[derive(Debug, Clone)]
pub struct FilterCriteria {
    pub max_price: Option<f64>,
    pub board_types: Option<Vec<String>>,
    pub free_cancellation: bool,
    pub hotel_ids: Option<Vec<String>>,
    pub room_type_contains: Option<String>
}

// Hotel search processor to implement
pub struct HotelSearchProcessor {
}

impl HotelSearchProcessor {
    // Create a new processor
    pub fn new() -> Self {
        Self {}
    }
    
    // Process an XML hotel search response and extract key information
    // The response can be large (1MB+) and should be processed efficiently
    pub fn process(&self, xml: &str) -> Result<ProcessedResponse, ProcessingError> {
        use quick_xml::events::Event;
        use quick_xml::Reader;
        
        let mut reader = Reader::from_str(xml);
        reader.trim_text(true);
        
        let mut hotels = Vec::new();
        let mut buf = Vec::new();
        
        // TODO: need to handle errors better here
        // parse hotel data from xml
        
        let mut current_hotel_id = String::new();
        let mut current_hotel_name = String::new();
        let mut current_board_type = String::new();
        let mut current_price_amount = 0.0;
        let mut current_price_currency = String::new();
        let mut current_room_code = String::new();
        let mut current_room_desc = String::new();
        let mut current_search_token = String::new();
        let mut is_refundable = true;
        
        let mut in_hotel = false;
        let mut in_mealplan = false;
        let mut in_option = false;
        let mut in_room = false;
        let mut current_tag: Option<Vec<u8>> = None;
        
        loop {
            match reader.read_event_into(&mut buf) {
                Ok(Event::Start(e)) => {
                    current_tag = Some(e.name().as_ref().to_vec());
                    match e.name().as_ref() {
                        b"Hotel" => {
                            in_hotel = true;
                            for attr in e.attributes() {
                                if let Ok(attr) = attr {
                                    match attr.key.as_ref() {
                                        b"code" => {
                                            current_hotel_id = String::from_utf8_lossy(&attr.value).to_string();
                                        }
                                        b"name" => {
                                            current_hotel_name = String::from_utf8_lossy(&attr.value).to_string();
                                        }
                                        _ => {}
                                    }
                                }
                            }
                        }
                        b"MealPlan" => {
                            in_mealplan = true;
                            for attr in e.attributes() {
                                if let Ok(attr) = attr {
                                    if attr.key.as_ref() == b"code" {
                                        current_board_type = String::from_utf8_lossy(&attr.value).to_string();
                                    }
                                }
                            }
                        }
                        b"Option" => {
                            in_option = true;
                            for attr in e.attributes() {
                                if let Ok(attr) = attr {
                                    if attr.key.as_ref() == b"paymentType" {
                                        // payment type stored but not used yet
                                    }
                                }
                            }
                        }
                        b"Room" => {
                            in_room = true;
                            for attr in e.attributes() {
                                if let Ok(attr) = attr {
                                    match attr.key.as_ref() {
                                        b"code" => {
                                            current_room_code = String::from_utf8_lossy(&attr.value).to_string();
                                        }
                                        b"description" => {
                                            current_room_desc = String::from_utf8_lossy(&attr.value).to_string();
                                        }
                                        b"nonRefundable" => {
                                            let val = String::from_utf8_lossy(&attr.value);
                                            is_refundable = val != "true";
                                        }
                                        _ => {}
                                    }
                                }
                            }
                        }
                        b"Price" => {
                            for attr in e.attributes() {
                                if let Ok(attr) = attr {
                                    match attr.key.as_ref() {
                                        b"currency" => {
                                            current_price_currency = String::from_utf8_lossy(&attr.value).to_string();
                                        }
                                        b"amount" => {
                                            let amount_str = String::from_utf8_lossy(&attr.value);
                                            current_price_amount = amount_str.parse().unwrap_or(0.0);
                                        }
                                        _ => {}
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
                Ok(Event::Text(e)) => {
                    if let Some(ref tag) = current_tag {
                        let text = e.unescape().unwrap_or_default().to_string();
                        match tag.as_slice() {
                            b"Parameter" => {
                                // will handle search_token later
                            }
                            _ => {}
                        }
                    }
                }
                Ok(Event::End(e)) => {
                    match e.name().as_ref() {
                        b"Room" => {
                            if in_room && in_option && in_mealplan && in_hotel {
                                if !current_hotel_id.is_empty() && !current_room_code.is_empty() {
                                    hotels.push(HotelOption {
                                        hotel_id: current_hotel_id.clone(),
                                        hotel_name: current_hotel_name.clone(),
                                        room_type: current_room_code.clone(),
                                        room_description: current_room_desc.clone(),
                                        board_type: current_board_type.clone(),
                                        price: Price {
                                            amount: current_price_amount,
                                            currency: current_price_currency.clone(),
                                        },
                                        cancellation_policies: Vec::new(),
                                        payment_type: "MerchantPay".to_string(),
                                        is_refundable,
                                        search_token: current_search_token.clone(),
                                    });
                                }
                            }
                            in_room = false;
                            current_room_code.clear();
                            current_room_desc.clear();
                        }
                        b"Hotel" => {
                            in_hotel = false;
                            current_hotel_id.clear();
                            current_hotel_name.clear();
                        }
                        b"MealPlan" => {
                            in_mealplan = false;
                            current_board_type.clear();
                        }
                        b"Option" => {
                            in_option = false;
                        }
                        _ => {}
                    }
                    current_tag = None;
                }
                Ok(Event::Eof) => break,
                Err(e) => return Err(ProcessingError::XmlParseError(format!("Parse error: {}", e))),
                _ => {}
            }
            buf.clear();
        }
        
        Ok(ProcessedResponse {
            search_id: "".to_string(),
            total_options: hotels.len(),
            hotels,
            currency: "".to_string(),
            nationality: "".to_string(),
            check_in: "".to_string(),
            check_out: "".to_string(),
        })
    }
    
    // Convert supplier JSON response to XML format
    pub fn convert_json_to_xml(&self, json_str: &str) -> Result<String, ProcessingError> {
        // Parse the JSON string into SupplierResponse
        let supplier_response: SupplierResponse = match serde_json::from_str(json_str) {
            Ok(response) => response,
            Err(e) => return Err(ProcessingError::JsonParseError(e.to_string())),
        };
        
        // Convert to XML format
        // In a real implementation, you would use a proper XML builder library
        // This is a simplified version for the assessment
        let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        xml.push_str("<AvailRS>\n");
        xml.push_str("  <Hotels>\n");
        
        for hotel in &supplier_response.hotels {
            xml.push_str(&format!("    <Hotel code=\"{}\", name=\"{}\">\n", 
                hotel.hotel_id, hotel.name));
            xml.push_str("      <MealPlans>\n");
            
            // Group rooms by board type
            let mut board_types = std::collections::HashMap::new();
            
            for room in &hotel.rooms {
                for rate in &room.rates {
                    let entries = board_types.entry(rate.board_type.clone())
                        .or_insert_with(Vec::new);
                    entries.push((room, rate));
                }
            }
            
            for (board_type, room_rates) in board_types {
                xml.push_str(&format!("        <MealPlan code=\"{}\">\n", board_type));
                xml.push_str("          <Options>\n");
                xml.push_str("            <Option type=\"Hotel\" paymentType=\"MerchantPay\" status=\"OK\">\n");
                xml.push_str(&format!("              <Price currency=\"{}\" amount=\"{}\" binding=\"false\" commission=\"-1\" minimumSellingPrice=\"-1\"/>\n", 
                    supplier_response.currency, room_rates[0].1.price));
                xml.push_str("              <Rooms>\n");
                
                for (room, rate) in room_rates {
                    xml.push_str(&format!("                <Room id=\"1#{}\" roomCandidateRefId=\"1\" code=\"{}\" description=\"{}\" numberOfUnits=\"1\" nonRefundable=\"false\">\n", 
                        room.room_id, room.room_id, room.name));
                    xml.push_str(&format!("                  <Price currency=\"{}\" amount=\"{}\" binding=\"false\" commission=\"-1\" minimumSellingPrice=\"-1\"/>\n", 
                        supplier_response.currency, rate.price));
                    
                    if !rate.cancellation_policies.is_empty() {
                        xml.push_str("                  <CancelPenalties nonRefundable=\"false\">\n");
                        
                        for policy in &rate.cancellation_policies {
                            xml.push_str("                    <CancelPenalty>\n");
                            xml.push_str("                      <HoursBefore>24</HoursBefore>\n"); // Simplified
                            xml.push_str(&format!("                      <Penalty type=\"Importe\" currency=\"{}\">{}</Penalty>\n", 
                                supplier_response.currency, policy.amount));
                            xml.push_str(&format!("                      <Deadline>{}</Deadline>\n", policy.from_date));
                            xml.push_str("                    </CancelPenalty>\n");
                        }
                        
                        xml.push_str("                  </CancelPenalties>\n");
                    }
                    
                    xml.push_str("                </Room>\n");
                }
                
                xml.push_str("              </Rooms>\n");
                xml.push_str("              <Parameters>\n");
                xml.push_str(&format!("                <Parameter key=\"search_token\" value=\"{}|||||{}\"/>\n", 
                    hotel.hotel_id, supplier_response.search_id));
                xml.push_str("              </Parameters>\n");
                xml.push_str("            </Option>\n");
                xml.push_str("          </Options>\n");
                xml.push_str("        </MealPlan>\n");
            }
            
            xml.push_str("      </MealPlans>\n");
            xml.push_str("    </Hotel>\n");
        }
        
        xml.push_str("  </Hotels>\n");
        xml.push_str("</AvailRS>\n");
        
        Ok(xml)
    }
    
    // Convert XML response to ProcessedResponse format
    pub fn xml_to_processed_response(&self, _xml: &str) -> Result<ProcessedResponse, ProcessingError> {
        // TODO: Implement this to convert XML to ProcessedResponse
        // This would be implemented in a real solution
        Err(ProcessingError::Other("Not implemented".to_string()))
    }
    
    // Extract hotel options that match the given criteria
    pub fn filter_options(&self, response: &ProcessedResponse, 
                     criteria: &FilterCriteria) -> Vec<HotelOption> {
        let mut filtered = Vec::new();
        
        for hotel in &response.hotels {
            // Apply filters
            let price_ok = criteria.max_price
                .map_or(true, |max| hotel.price.amount <= max);
                
            let board_type_ok = criteria.board_types
                .as_ref()
                .map_or(true, |types| types.contains(&hotel.board_type));
                
            let cancellation_ok = !criteria.free_cancellation || hotel.is_refundable;
            
            let hotel_id_ok = criteria.hotel_ids
                .as_ref()
                .map_or(true, |ids| ids.contains(&hotel.hotel_id));
                
            let room_type_ok = criteria.room_type_contains
                .as_ref()
                .map_or(true, |substring| hotel.room_type.contains(substring));
                
            if price_ok && board_type_ok && cancellation_ok && hotel_id_ok && room_type_ok {
                filtered.push(hotel.clone());
            }
        }
        
        filtered
    }
    
    // Helper method to load the sample JSON response
    pub fn load_sample_json(&self) -> Result<String, ProcessingError> {
        match std::fs::read_to_string(SAMPLE_JSON_PATH) {
            Ok(content) => Ok(content),
            Err(e) => Err(ProcessingError::IoError(e))
        }
    }
    
    // Helper method to load the sample response XML
    pub fn load_sample_response(&self) -> Result<String, ProcessingError> {
        match std::fs::read_to_string(SAMPLE_XML_PATH) {
            Ok(content) => Ok(content),
            Err(e) => Err(ProcessingError::IoError(e))
        }
    }
    
    // Helper method to load the sample request XML
    pub fn load_sample_request(&self) -> Result<String, ProcessingError> {
        match std::fs::read_to_string(SAMPLE_REQUEST_PATH) {
            Ok(content) => Ok(content),
            Err(e) => Err(ProcessingError::IoError(e))
        }
    }
    
    // extract params from request xml
    pub fn extract_search_params(&self, request_xml: &str) -> Result<(String, String, String, String), ProcessingError> {
        use quick_xml::events::Event;
        use quick_xml::Reader;
        
        let mut reader = Reader::from_str(request_xml);
        reader.trim_text(true);
        
        let mut currency = String::new();
        let mut nationality = String::new();
        let mut start_date = String::new();
        let mut end_date = String::new();
        let mut buf = Vec::new();
        let mut current_tag: Option<Vec<u8>> = None;
        
        loop {
            match reader.read_event_into(&mut buf) {
                Ok(Event::Start(e)) => {
                    current_tag = Some(e.name().as_ref().to_vec());
                }
                Ok(Event::Text(e)) => {
                    if let Some(ref tag) = current_tag {
                        let text = e.unescape().unwrap_or_default().to_string();
                        match tag.as_slice() {
                            b"Currency" => currency = text,
                            b"Nationality" => nationality = text,
                            b"StartDate" => start_date = text,
                            b"EndDate" => end_date = text,
                            _ => {}
                        }
                    }
                }
                Ok(Event::End(_)) => {
                    current_tag = None;
                }
                Ok(Event::Eof) => break,
                Err(e) => return Err(ProcessingError::XmlParseError(format!("Parse error: {}", e))),
                _ => {}
            }
            buf.clear();
        }
        
        if currency.is_empty() || nationality.is_empty() || start_date.is_empty() || end_date.is_empty() {
            return Err(ProcessingError::MissingRequiredField("One or more required fields missing".to_string()));
        }
        
        Ok((currency, nationality, start_date, end_date))
    }
}

// Sample file paths (the actual files are stored in the samples directory)
pub const SAMPLE_XML_PATH: &str = "samples/hotel_search_response.xml";
pub const SAMPLE_REQUEST_PATH: &str = "samples/hotel_search_request.xml";
pub const SAMPLE_JSON_PATH: &str = "samples/supplier_response.json";

// A small sample for inline testing
pub const SMALL_SAMPLE_XML: &str = r#"
<AvailRS>
  <Hotels>
    <Hotel code="39776757" name="Days Inn By Wyndham Fargo">
      <MealPlans>
        <MealPlan code="RO">
          <Options>
            <Option type="Hotel" paymentType="MerchantPay" status="OK">
              <Price currency="GBP" amount="84.82" binding="false" commission="-1" minimumSellingPrice="-1"/>
              <Rooms>
                <Room id="1#ND1" roomCandidateRefId="1" code="ND1" description="ROOM, QUEEN BED" numberOfUnits="1" nonRefundable="false">
                  <Price currency="GBP" amount="84.82" binding="false" commission="-1" minimumSellingPrice="-1"/>
                  <CancelPenalties nonRefundable="false">
                    <CancelPenalty>
                      <HoursBefore>26</HoursBefore>
                      <Penalty type="Importe" currency="GBP">84.82</Penalty>
                      <Deadline>2025-06-10T10:00:00Z</Deadline>
                    </CancelPenalty>
                  </CancelPenalties>
                </Room>
              </Rooms>
              <Parameters>
                <Parameter key="search_token" value="39776757|2025-06-11|2025-06-12|A|US|GBP"/>
              </Parameters>
            </Option>
          </Options>
        </MealPlan>
      </MealPlans>
    </Hotel>
  </Hotels>
</AvailRS>
"#;

#[cfg(test)]
mod tests {
    use super::*;
    
    // Test JSON to XML conversion
    #[test]
    fn test_json_to_xml_conversion() {
        let processor = HotelSearchProcessor::new();
        
        // Sample JSON for testing
        let sample_json = r#"{
            "hotels": [
                {
                    "hotel_id": "12345",
                    "name": "Test Hotel",
                    "category": 4,
                    "destination_code": "NYC",
                    "rooms": [
                        {
                            "room_id": "DBL",
                            "name": "Double Room",
                            "capacity": {
                                "adults": 2,
                                "children": 0
                            },
                            "rates": [
                                {
                                    "rate_id": "R1",
                                    "board_type": "BB",
                                    "price": 120.50,
                                    "booking_code": "TESTCODE",
                                    "cancellation_policies": [
                                        {
                                            "from_date": "2023-12-01T00:00:00Z",
                                            "amount": 50.25
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ],
            "search_id": "SEARCH123",
            "currency": "USD",
            "timestamp": "2023-11-15T10:30:00Z"
        }"#;
        
        // Convert JSON to XML
        let xml_result = processor.convert_json_to_xml(sample_json);
        assert!(xml_result.is_ok(), "JSON to XML conversion failed: {:?}", xml_result.err());
        
        let xml = xml_result.unwrap();
        
        // Verify XML structure
        assert!(xml.contains("<AvailRS>"));
        assert!(xml.contains("<Hotel code=\"12345\""));
        assert!(xml.contains("<MealPlan code=\"BB\">"));
        assert!(xml.contains("<Room id=\"1#DBL\"")); 
        assert!(xml.contains("<Price currency=\"USD\" amount=\"120.5\""));
        assert!(xml.contains("<Deadline>2023-12-01T00:00:00Z</Deadline>"));
        assert!(xml.contains("<Parameter key=\"search_token\" value=\"12345|||||SEARCH123\"/>"));
    }
    
    // Test loading the sample JSON file
    #[test]
    fn test_load_sample_json() {
        let processor = HotelSearchProcessor::new();
        let result = processor.load_sample_json();
        assert!(result.is_ok(), "Failed to load sample JSON: {:?}", result.err());
        
        // Verify it's a valid JSON
        let json = result.unwrap();
        let parse_result = serde_json::from_str::<SupplierResponse>(&json);
        assert!(parse_result.is_ok(), "Failed to parse sample JSON: {:?}", parse_result.err());
    }
    
    // Test full JSON to XML conversion workflow using sample files
    #[test]
    fn test_sample_json_to_xml_workflow() {
        let processor = HotelSearchProcessor::new();
        
        // Load sample JSON
        let json_result = processor.load_sample_json();
        assert!(json_result.is_ok(), "Failed to load sample JSON: {:?}", json_result.err());
        
        // Convert JSON to XML
        let xml_result = processor.convert_json_to_xml(&json_result.unwrap());
        assert!(xml_result.is_ok(), "JSON to XML conversion failed: {:?}", xml_result.err());
        
        let xml = xml_result.unwrap();
        assert!(xml.contains("<AvailRS>"));
        assert!(xml.contains("<Hotels>"));
        // The actual content will depend on the sample JSON file
    }
    
    // Example test for processing XML (commented out as it would be implemented by candidates)
    // #[test]
    // fn test_process_xml() {
    //     let processor = HotelSearchProcessor::new();
    //     let result = processor.process(SMALL_SAMPLE_XML);
    //     
    //     assert!(result.is_ok());
    //     let response = result.unwrap();
    //     
    //     // Check basic response properties
    //     assert_eq!(response.hotels.len(), 1);
    //     
    //     // Check first hotel
    //     let hotel = &response.hotels[0];
    //     assert_eq!(hotel.hotel_id, "39776757");
    //     assert_eq!(hotel.hotel_name, "Days Inn By Wyndham Fargo");
    //     assert_eq!(hotel.board_type, "RO");
    //     assert_eq!(hotel.price.amount, 84.82);
    //     assert_eq!(hotel.price.currency, "GBP");
    //     assert_eq!(hotel.is_refundable, true);
    //     
    //     // Check cancellation policy
    //     assert_eq!(hotel.cancellation_policies.len(), 1);
    //     let policy = &hotel.cancellation_policies[0];
    //     assert_eq!(policy.hours_before, 26);
    //     assert_eq!(policy.penalty_amount, 84.82);
    //     assert_eq!(policy.currency, "GBP");
    // }
    
    // Example test for filtering options (commented out as it would be implemented by candidates)
    // #[test]
    // fn test_filter_options() {
    //     let processor = HotelSearchProcessor::new();
    //     
    //     // Create a sample processed response with multiple hotels
    //     let mut response = ProcessedResponse {
    //         search_id: "test_search".to_string(),
    //         total_options: 3,
    //         hotels: Vec::new(),
    //         currency: "GBP".to_string(),
    //         nationality: "GB".to_string(),
    //         check_in: "2025-06-01".to_string(),
    //         check_out: "2025-06-05".to_string(),
    //     };
    //     
    //     // Add sample hotels with different properties
    //     response.hotels.push(HotelOption {
    //         hotel_id: "hotel1".to_string(),
    //         hotel_name: "Luxury Hotel".to_string(),
    //         room_type: "Deluxe King".to_string(),
    //         room_description: "Spacious room with king bed".to_string(),
    //         board_type: "BB".to_string(), // Bed & Breakfast
    //         price: Price { amount: 150.0, currency: "GBP".to_string() },
    //         cancellation_policies: vec![
    //             CancellationPolicy {
    //                 deadline: "2025-05-30T00:00:00Z".to_string(),
    //                 penalty_amount: 75.0,
    //                 currency: "GBP".to_string(),
    //                 hours_before: 48,
    //                 penalty_type: "Importe".to_string(),
    //             }
    //         ],
    //         payment_type: "MerchantPay".to_string(),
    //         is_refundable: true,
    //         search_token: "token1".to_string(),
    //     });
    //     
    //     response.hotels.push(HotelOption {
    //         hotel_id: "hotel2".to_string(),
    //         hotel_name: "Budget Inn".to_string(),
    //         room_type: "Standard Twin".to_string(),
    //         room_description: "Basic room with twin beds".to_string(),
    //         board_type: "RO".to_string(), // Room Only
    //         price: Price { amount: 80.0, currency: "GBP".to_string() },
    //         cancellation_policies: vec![],
    //         payment_type: "MerchantPay".to_string(),
    //         is_refundable: false,
    //         search_token: "token2".to_string(),
    //     });
    //     
    //     response.hotels.push(HotelOption {
    //         hotel_id: "hotel3".to_string(),
    //         hotel_name: "Resort Spa".to_string(),
    //         room_type: "Premium Suite".to_string(),
    //         room_description: "Luxury suite with ocean view".to_string(),
    //         board_type: "HB".to_string(), // Half Board
    //         price: Price { amount: 250.0, currency: "GBP".to_string() },
    //         cancellation_policies: vec![
    //             CancellationPolicy {
    //                 deadline: "2025-05-25T00:00:00Z".to_string(),
    //                 penalty_amount: 100.0,
    //                 currency: "GBP".to_string(),
    //                 hours_before: 168,
    //                 penalty_type: "Importe".to_string(),
    //             }
    //         ],
    //         payment_type: "MerchantPay".to_string(),
    //         is_refundable: true,
    //         search_token: "token3".to_string(),
    //     });
    //     
    //     // Test 1: Filter by max price
    //     let criteria1 = FilterCriteria {
    //         max_price: Some(100.0),
    //         board_types: None,
    //         free_cancellation: false,
    //         hotel_ids: None,
    //         room_type_contains: None,
    //     };
    //     
    //     let results1 = processor.filter_options(&response, &criteria1);
    //     assert_eq!(results1.len(), 1);
    //     assert_eq!(results1[0].hotel_id, "hotel2");
    //     
    //     // Test 2: Filter by board type
    //     let criteria2 = FilterCriteria {
    //         max_price: None,
    //         board_types: Some(vec!["BB".to_string(), "HB".to_string()]),
    //         free_cancellation: false,
    //         hotel_ids: None,
    //         room_type_contains: None,
    //     };
    //     
    //     let results2 = processor.filter_options(&response, &criteria2);
    //     assert_eq!(results2.len(), 2);
    //     assert!(results2.iter().any(|h| h.hotel_id == "hotel1"));
    //     assert!(results2.iter().any(|h| h.hotel_id == "hotel3"));
    //     
    //     // Test 3: Filter by free cancellation
    //     let criteria3 = FilterCriteria {
    //         max_price: None,
    //         board_types: None,
    //         free_cancellation: true,
    //         hotel_ids: None,
    //         room_type_contains: None,
    //     };
    //     
    //     let results3 = processor.filter_options(&response, &criteria3);
    //     assert_eq!(results3.len(), 2);
    //     assert!(results3.iter().all(|h| h.is_refundable));
    //     
    //     // Test 4: Filter by room type
    //     let criteria4 = FilterCriteria {
    //         max_price: None,
    //         board_types: None,
    //         free_cancellation: false,
    //         hotel_ids: None,
    //         room_type_contains: Some("Suite".to_string()),
    //     };
    //     
    //     let results4 = processor.filter_options(&response, &criteria4);
    //     assert_eq!(results4.len(), 1);
    //     assert_eq!(results4[0].hotel_id, "hotel3");
    //     
    //     // Test 5: Combined filters
    //     let criteria5 = FilterCriteria {
    //         max_price: Some(300.0),
    //         board_types: Some(vec!["HB".to_string()]),
    //         free_cancellation: true,
    //         hotel_ids: None,
    //         room_type_contains: Some("Suite".to_string()),
    //     };
    //     
    //     let results5 = processor.filter_options(&response, &criteria5);
    //     assert_eq!(results5.len(), 1);
    //     assert_eq!(results5[0].hotel_id, "hotel3");
    // }

    #[test]
    fn test_load_sample_response() {
        let processor = HotelSearchProcessor::new();
        let result = processor.load_sample_response();
        assert!(result.is_ok(), "Failed to load sample XML response: {:?}", result.err());
    }
    
    #[test]
    fn test_load_sample_request() {
        let processor = HotelSearchProcessor::new();
        let result = processor.load_sample_request();
        assert!(result.is_ok(), "Failed to load sample XML request: {:?}", result.err());
    }
    
    #[test]
    fn test_extract_search_params() {
        let processor = HotelSearchProcessor::new();
        let sample_xml = processor.load_sample_request().unwrap();
        let result = processor.extract_search_params(&sample_xml);
        
        assert!(result.is_ok(), "Failed to extract params: {:?}", result.err());
        let (currency, nationality, start_date, end_date) = result.unwrap();
        assert_eq!(currency, "GBP");
        assert_eq!(nationality, "US");
        assert_eq!(start_date, "11/06/2025");
        assert_eq!(end_date, "12/06/2025");
    }
}
