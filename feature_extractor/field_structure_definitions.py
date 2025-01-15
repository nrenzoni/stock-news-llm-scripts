from pydantic import BaseModel, Field
from typing import Literal

FinancialEvent = Literal[
    "FutureAdjustments",
    "DividendChange",
    "ExpectationMisses",
    "GuidanceUpdates",
    "SentimentAnalysis",
    "ProfitOrLoss",
    "PerShareEarnings",
    "EarningsBeatOrMiss",
    "AnalystCount",
    "ConsensusEarningsEstimate",
    "TotalRevenue",
    "RevenueBeatOrMiss",
    "RevenueExpectation",
    "QuarterlyRevenueExpectation",
    "FiscalQuarter",
    "LongTermGrowthTarget",
    "AnnualRevenueProjection",
    "ExecutiveChange",
    "ExecutivePosition",
    "OutgoingExecutive",
    "IncomingExecutive",
    "ChangeReason",
    "CorporateRestructuring",
    "HealthUpdates",
    "SectorTags",
    "IndustryTags",
    "InnovationAnnouncement",
    "ProductOrTechImprovement",
    "CompetitiveProductAdvantage",
    "CorporateAcquisition",
    "FinancialReport",
    "TargetPriceIncrease",
    "BondYieldRelations",
    "StockOffering",
    "OfferingAmount",
    "OfferingType",
    "PublicOrPrivateOffering",
    "RegulatoryApproval",
    "GovernmentOversight",
    "StockBuyback",
    "TargetPriceAdjustment",
    "RevisedPriceTarget",
    "OrderModifications",
    "OrderProduct",
    "PreviousOrderQuantity",
    "RevisedOrderQuantity",
    "PreviousOrderPrice",
    "RevisedOrderPrice",
    "BusinessDeal",
    "DealCancellationThreat",
    "DividendAdjustment",
    "CorporateUpdate",
    "EfficacyResults",
    "AnalystRatingUpdate",
    "FutureAnnouncement",
    "ClinicalTrialResults",
    "EarlyTrialOutcome",
    "FinalTrialOutcome",
    "TrialPhase",
    "ParticipantCount",
    "OutcomeEvaluation",
    "LegalSettlement",
    "PendingLitigation",
    "CompleteSettlement",
    "ShortSellerReport",
    "MergerBlock",
    "FutureTargetProjection",
    "GovernmentLawsuit",
    "IndexMembershipChange",
    "DelistingAnnouncement",
    "TickerSymbolUpdate",
    "ScientificFindings",
    "ResearchRelation",
    "RegulatoryCircumvention",
    "ExportRegulation"
]


class SymbolLookup(BaseModel):
    symbol: str
    stock_exchanges: list[str]


class FinancialEventWithSymbol(BaseModel):
    financial_event: FinancialEvent
    symbol: SymbolLookup


class Sentiment(BaseModel):
    sentiment: str
    sentiment_confidence: float = Field(
        description="Confidence of the sentiment between 0 and 1, where 1 is the most confident"
    )
    sentiment_score: float = Field(
        description="Score of the sentiment between -100 and 100, where 100 is the most positive sentiment and -100 is the most negative sentiment"
    )
    chain_of_thought_reasoning: str


class SignificantKeyword(BaseModel):
    keyword: str
    keyword_score: float = Field(
        ...,
        description="Score of the keyword between 0 and 100, where 100 is the most significant keyword in the text"
    )


class ExternalLink(BaseModel):
    url: str
    link_text: str
    type: Literal['pdf', 'article', 'video', 'image', 'other']
    metadata: list[str]


class Entity(BaseModel):
    entity: str
    entity_type: str
    entity_description: str


class Relationship(BaseModel):
    source_entity: str
    relationship_descriptions: list[str] = Field(
        description="Description of the relationship. Example: ['is a subsidiary of']. Separate multiple relationships, example: instead of 'is CEO and CFO of', use ['is CEO of', 'is CFO of']")
    relationship_descriptions_condensed: list[str] = Field(
        description="condensed versions of relationship_descriptions. Don't include 'is' and 'of' if not necessary. Example: instead of 'is a subsidiary of', use 'subsidiary', ")
    target_entity: str
    relationship_strength: float = Field(description="Strength of the relationship between 0 and 1")


class FinancialNewsExtractedData(BaseModel):
    # title: str
    # publish_datetime: str = Field(..., description="ISO 8601 format")
    # publisher_source: str
    summary: str
    main_company: str = Field(
        ..., description="Focus company of the article")
    financial_event_with_symbols: list[FinancialEventWithSymbol]
    keywords: list[SignificantKeyword]
    sentiments: list[Sentiment]
    article_language: str = Field(
        ..., description="ISO 639-1 language code")
    external_links: list[ExternalLink] = Field(
        ..., description="Links to external resources, at most 1 per url")
    entities: list[Entity] = Field(
        ..., description="identified entities in the text")
    relationships: list[Relationship] = Field(
        ...,
        description="*clearly identifiable* relationships between previously identified entities")
